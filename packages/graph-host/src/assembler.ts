/**
 * The Agent Graph host assembly: wires the P1–P5 slices to the injected real
 * harness facades and publishes the service set the supervisor tools consume.
 * @module
 */

import { randomUUID } from 'node:crypto'
import {
  GraphControlStore,
  type AgentGraphScheduleUpdateRequest,
} from '@hy-sde-org/dsh-graph-control'
import { createGraphOperatorExecutor } from '@hy-sde-org/dsh-graph-executor'
import {
  readCommittedAgentGraphProjection,
  type AgentGraphExecutor,
  type AgentGraphRecord,
  type AgentGraphRecordSourceEvent,
  type AgentGraphReconciliationTopology,
  type AgentGraphRunClaimedIntentInput,
  type AgentGraphSupervisorObservation,
} from '@hy-sde-org/dsh-graph-stream'
import {
  GraphWakeRuntime,
  type GraphWakeDeliver,
  type GraphWakeDeliveryOutcome,
  type GraphWakeDue,
} from '@hy-sde-org/dsh-graph-wakes'
import {
  AgentGraphController,
  type AgentGraphControllerOptions,
  type AgentGraphControllerScheduleResult,
} from '@hy-sde-org/dsh-tool-graph'
import { GraphHostChildRunner } from './child-runner.ts'
import { buildSessionGraphProjection } from './projection.ts'
import {
  GraphRunIdentityLedger,
  InProcessGraphRecordSource,
} from './records.ts'
import type {
  GraphHostAssemblerOptions,
  GraphHostServices,
} from './types.ts'
import type { SessionGraphProjection } from '@hy-sde-org/dsh-graph-projection/types'
import { GraphHostWorktreePool } from './worktree-pool.ts'

/**
 * Host-level exactly-once activation guard: a re-drive after a finished (or
 * in-flight) activation must not start a second child run for the same durable
 * claim. P2's seam contract makes the executor responsible for claim
 * idempotency; the P3 executor serializes per operator but does not memoize
 * claims, so the host provides the guard here (process-local — the durable
 * claim row stays the restart authority, like every other record fold).
 */
class OncePerClaimGraphExecutor implements AgentGraphExecutor {
  private readonly finished = new Map<string, readonly AgentGraphRecord[]>()
  private readonly running = new Map<string, Promise<readonly AgentGraphRecord[]>>()

  constructor(private readonly inner: AgentGraphExecutor) { }

  provisionOperator(request: Parameters<AgentGraphExecutor['provisionOperator']>[0]) {
    return this.inner.provisionOperator(request)
  }

  runClaimedAgentGraphIntent(input: AgentGraphRunClaimedIntentInput): Promise<AgentGraphRecord[]> {
    const claimId = input.claim.claimId
    const done = this.finished.get(claimId)
    if (done !== undefined) return Promise.resolve([...done])
    const inFlight = this.running.get(claimId)
    if (inFlight !== undefined) return inFlight.then(records => [...records])
    const run = this.inner.runClaimedAgentGraphIntent(input).then(
      (records) => {
        this.finished.set(claimId, records)
        this.running.delete(claimId)
        return records
      },
      (error: unknown) => {
        this.running.delete(claimId)
        throw error
      },
    )
    this.running.set(claimId, run)
    return run.then(records => [...records])
  }

  stopSession(sessionId: string, opts?: { source?: 'graph_supervisor' }): Promise<void> {
    return this.inner.stopSession(sessionId, opts)
  }
}

/**
 * Signal a provider-confirmed context overflow out of a deliver attempt. The
 * wake runtime runs its one-compaction recovery when the outcome carries this
 * marker; a second overflow within the same wake yields the bounded partial
 * snapshot (`partialResult: true`).
 */
export class GraphHostContextOverflowError extends Error {
  readonly overflow = true

  constructor(message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = 'GraphHostContextOverflowError'
  }
}

/** True when a failure is a provider-confirmed context overflow (owned marker or conventional property). */
export { OncePerClaimGraphExecutor }

export function isContextOverflow(error: unknown): boolean {
  if (error instanceof GraphHostContextOverflowError) return true
  return (
    typeof error === 'object' &&
    error !== null &&
    'overflow' in error &&
    error.overflow === true
  )
}

/**
 * Assemble the graph host services for one root session. Opens the graph
 * control unit, builds the executor (child runner over `subagents`, worktree
 * pool with adoption, identity-ledger record sink), the controller with a
 * whole-graph observation seam, and the wake runtime with re-drive delivery.
 */
export async function createGraphHostServices(
  input: GraphHostAssemblerOptions,
): Promise<GraphHostServices> {
  const clock = input.clock ?? defaultClock
  const newId = input.newId ?? generateId
  const maxNewActivations = input.maxNewActivations ?? DEFAULT_MAX_NEW_ACTIVATIONS
  const onError = input.onError ?? noop
  const unit = await input.storage.open(GraphControlStore.descriptor)
  const store = await GraphControlStore.open(unit)
  const recordSource = new InProcessGraphRecordSource()
  const ledger = new GraphRunIdentityLedger()
  const childRunner = new GraphHostChildRunner({
    subagents: input.subagents,
    resolveParentAgent: input.resolveParentAgent,
    rootSessionId: input.rootSessionId,
    ledger,
  })
  const pool = new GraphHostWorktreePool(input.worktrees)

  // The P3 record sink carries no operator/session identity; the ledger
  // restores it from the child start the executor itself performed.
  const recordSink = (event: AgentGraphRecordSourceEvent): Promise<void> => {
    const identity = ledger.take(event.runId)
    if (identity === undefined) {
      onError(
        new Error(
          `agent graph host: no activation identity for run ${event.runId}; terminal event dropped`,
        ),
      )
      return Promise.resolve()
    }
    recordSource.submit(identity, event)
    return Promise.resolve()
  }

  const executor = new OncePerClaimGraphExecutor(
    createGraphOperatorExecutor({
      store,
      pool,
      childRunner,
      recordSink,
      newId,
    }),
  )

  // The controller is constructed before the services object exists, so
  // emission closures resolve it through this holder (drives only start after
  // the assembler returns, through the controller or a wake delivery).
  const held: { services?: GraphHostServices } = {}
  const lastChangeFingerprint = new Map<string, string>()
  const changeQueues = new Map<string, Promise<void>>()

  /** One emission body: best-effort fresh `graph/change`, deduped by fingerprint. */
  const emitChangeOnce = async (graphId: string, force: boolean): Promise<void> => {
    const services = held.services
    if (services === undefined) return
    const snapshot = await services.snapshotFor(graphId)
    const fingerprint = changeFingerprintOf(snapshot)
    if (!force && lastChangeFingerprint.get(graphId) === fingerprint) return
    lastChangeFingerprint.set(graphId, fingerprint)
    await services.emitGraphChange(
      input.rootSessionId,
      graphId,
      snapshot,
      snapshot.revision,
    )
  }

  /** Best-effort `graph/change` emission, serialized per graph so the fingerprint dedup is race-free. */
  const emitChange = (graphId: string, force = false): Promise<void> => {
    const previous = changeQueues.get(graphId) ?? Promise.resolve()
    const run = previous.then(
      () => emitChangeOnce(graphId, force),
      () => emitChangeOnce(graphId, force),
    )
    changeQueues.set(
      graphId,
      run.then(
        () => undefined,
        () => undefined,
      ),
    )
    return run
  }

  const observeGraph = async (
    topology: AgentGraphReconciliationTopology,
  ): Promise<AgentGraphSupervisorObservation> => {
    const state = await readCommittedAgentGraphProjection(
      topology.graphId,
      topology.operators,
      recordSource,
    )
    void emitChange(topology.graphId).catch(onError)
    return {
      graphId: topology.graphId,
      records: state.records,
      operators: state.operators,
    }
  }

  const controller = new GraphHostRecordingController(
    {
      store,
      rootSessionId: input.rootSessionId,
      newId,
      observeGraph,
      options: {
        executor,
        recordSource,
        newId,
        maxNewActivations,
      },
    },
    (graphId) => {
      void emitChange(graphId).catch(onError)
    },
  )

  const overflowAttempts = new Map<string, number>()
  const wakeDeliver: GraphWakeDeliver = async (due: GraphWakeDue): Promise<GraphWakeDeliveryOutcome> => {
    const services = held.services
    if (services === undefined) {
      throw new Error('agent graph host: services not assembled yet')
    }
    const coordinator = services.controller.getOrCreate(due.graphId)
    if (coordinator.isClosed()) return { kind: 'superseded' }
    try {
      await coordinator.reconcileAndWait()
      // The post-reconcile projection read is part of the delivery: an
      // overflow-marked failure there (same storage the reconcile reads) is the
      // same recoverable overflow, so it stays inside the mapping below.
      await emitChange(due.graphId, true)
    } catch (error: unknown) {
      return overflowOutcomeOf(due, error, overflowAttempts, () => emitChange(due.graphId, true))
    }
    overflowAttempts.delete(due.wakeId)
    return { kind: 'delivered' }
  }

  const wakeRuntime = new GraphWakeRuntime({
    store,
    deliver: wakeDeliver,
    onCompact: sessionId => input.compaction.request(sessionId),
    now: () => clock.now(),
    observeIdle: onIdle => input.idle.observe(input.rootSessionId, onIdle),
    onError,
  })

  let wakeStarted = false
  const services: GraphHostServices = {
    store,
    controller,
    executor,
    wakeRuntime,
    async snapshotFor(graphId: string): Promise<SessionGraphProjection> {
      const snapshot = await controller.snapshot(graphId)
      const wakes = await store.listUnsettledSupervisorWakes()
      const pendingWake = wakes.some(
        wake =>
          wake.graphId === graphId &&
          (wake.status === 'pending' || wake.status === 'retryable_failed'),
      )
      return buildSessionGraphProjection({
        graphId,
        snapshot,
        pendingWake,
        now: clock.now(),
      })
    },
    async emitGraphChange(
      sessionId: string,
      graphId: string,
      snapshot: SessionGraphProjection,
      revision: number,
    ): Promise<boolean> {
      return input.sessionEvents.appendGraphChange(sessionId, {
        graphId,
        snapshot,
        revision,
      })
    },
    attachGraph(graphId: string): Promise<void> {
      controller.getOrCreate(graphId)
      if (!wakeStarted) {
        wakeRuntime.start(input.rootSessionId)
        wakeStarted = true
      }
      return Promise.resolve()
    },
    async dispose(): Promise<void> {
      await wakeRuntime.stop()
      await childRunner.stopAll()
      await store.close()
    },
  }
  held.services = services
  return services
}

/**
 * Schedule-path emission: the tool layer calls `schedule`/`stop` on the
 * controller, and both commit through `schedule`, so one emission per commit
 * keeps the session graph projection fresh.
 */
class GraphHostRecordingController extends AgentGraphController {
  private readonly afterCommit: (graphId: string) => void

  constructor(
    options: AgentGraphControllerOptions,
    afterCommit: (graphId: string) => void,
  ) {
    super(options)
    this.afterCommit = afterCommit
  }

  override async schedule(
    graphId: string,
    request: AgentGraphScheduleUpdateRequest,
  ): Promise<AgentGraphControllerScheduleResult> {
    const result = await super.schedule(graphId, request)
    // Fire-and-forget: emission is a best-effort projection refresh.
    this.afterCommit(graphId)
    return result
  }
}

/**
 * Outcome mapping of a failed delivery: an overflow triggers the runtime's
 * one-compaction recovery; the second overflow of the same wake carries the
 * bounded partial snapshot so the runtime can exhaust the wake.
 */
async function overflowOutcomeOf(
  due: GraphWakeDue,
  error: unknown,
  attempts: Map<string, number>,
  emitPartial: () => Promise<void>,
): Promise<GraphWakeDeliveryOutcome> {
  const failureReason = renderError(error)
  if (!isContextOverflow(error)) {
    attempts.delete(due.wakeId)
    return { kind: 'retryable_failed', failureReason }
  }
  const count = (attempts.get(due.wakeId) ?? 0) + 1
  attempts.set(due.wakeId, count)
  if (count <= 1) {
    return { kind: 'retryable_failed', overflow: true, failureReason }
  }
  try {
    await emitPartial()
  } catch (emitError: unknown) {
    // The partial snapshot is best-effort; never mask the overflow itself.
    void emitError
  }
  return { kind: 'retryable_failed', overflow: true, partialResult: true, failureReason }
}

function renderError(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function changeFingerprintOf(snapshot: SessionGraphProjection): string {
  return [
    snapshot.revision,
    snapshot.status,
    snapshot.pendingWake,
    snapshot.work.map(workish => `${workish.workId}:${workish.status}`).join(','),
    snapshot.omitted.work,
    snapshot.omitted.records,
  ].join('|')
}

const DEFAULT_MAX_NEW_ACTIVATIONS = 4

const defaultClock: { now(): number } = { now: () => Date.now() }

function generateId(): string {
  return randomUUID()
}

function noop(_error: unknown): void { }
