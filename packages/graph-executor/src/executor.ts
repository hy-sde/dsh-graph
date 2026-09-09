/**
 * Child-operator executor adapter (Maka port, slice P3): the
 * {@link AgentGraphOperatorExecutor} turns the coordinator's executor calls
 * into (a) a deterministic worktree lease + durable binding per provision, and
 * (b) one serialized child run per operator that settles into a terminal
 * record. The subagent runner and worktree pool are injected seams; the store
 * is the durable authority for both provisions and bindings.
 *
 * Idempotency contract: `provisionOperator` derives `provisionKey(request)` =
 * `graph_operator_lease_<sha256(graphId, workId, provisionFingerprint)[32]>`,
 * so a retry of the same provision adopts the same lease key; `pool.acquire`
 * is idempotent per key (process-local by design — the binding row is the
 * durable hint a real pool consults after a restart). Worktrees intentionally
 * survive terminal runs (Maka contract), so this slice never releases a lease;
 * release is wired for graph teardown in a later slice.
 * @module
 */

import type {
  AgentGraphOperatorBinding,
  AgentGraphOperatorProvision,
  AgentGraphOperatorProvisionRequest,
  AgentGraphOperatorProvisionResult,
  GraphControlStore,
} from '@hy-sde-org/dsh-graph-control'
import { graphRecordId, stableHash32, truncateUtf8 } from '@hy-sde-org/dsh-graph-stream'
import type {
  AgentGraphExecutor,
  AgentGraphRecord,
  AgentGraphRecordSourceEvent,
  AgentGraphRunClaimedIntentInput,
} from '@hy-sde-org/dsh-graph-stream'
import type {
  AgentGraphOperatorExecutorOptions,
  AgentGraphChildRun,
  GraphOperatorChildRunner,
  GraphOperatorChildStartInput,
  GraphOperatorWorktreeLease,
  GraphOperatorWorktreePool,
} from './types.ts'

const TERMINAL_SUMMARY_MAX_BYTES = 16 * 1024

/** Deterministic lease key of one provision; a retry reuses it verbatim. */
export function provisionKey(request: AgentGraphOperatorProvisionRequest): string {
  return `graph_operator_lease_${stableHash32({
    graphId: request.graphId,
    workId: request.workId,
    provisionFingerprint: request.provisionFingerprint,
  })}`
}

/**
 * The process-local coordinator-facing executor surface: provision (lease +
 * binding), run one claimed intent (serialized per operator, terminal record
 * via {@link AgentGraphOperatorExecutorOptions.recordSink}), and stop a child
 * session.
 */
export class AgentGraphOperatorExecutor implements AgentGraphExecutor {
  private readonly store: GraphControlStore
  private readonly pool: GraphOperatorWorktreePool
  private readonly childRunner: GraphOperatorChildRunner
  private readonly recordSink: ((event: AgentGraphRecordSourceEvent) => Promise<void>) | undefined
  private readonly newId: () => string
  private readonly limiter: ConcurrencyLimiter | undefined
  private readonly operatorTails = new Map<string, Promise<void>>()

  constructor(options: AgentGraphOperatorExecutorOptions) {
    this.store = options.store
    this.pool = options.pool
    this.childRunner = options.childRunner
    this.recordSink = options.recordSink
    this.newId = options.newId
    this.limiter =
      options.concurrencyHint === undefined
        ? undefined
        : validateConcurrencyHint(options.concurrencyHint)
  }

  /**
   * Provision one operator child: acquire (or adopt) the deterministic
   * worktree lease, persist the binding, then commit the durable provision
   * row through the store. Returns `undefined` when no lease could be
   * acquired — the caller defers the work to a later drive.
   */
  async provisionOperator(
    request: AgentGraphOperatorProvisionRequest,
  ): Promise<AgentGraphOperatorProvisionResult | undefined> {
    const lease = await this.acquireLease(request)
    if (lease === undefined) return undefined
    await this.store.bindOperatorWorktree({
      graphId: request.graphId,
      workId: request.workId,
      provisionId: request.provisionId,
      leaseId: lease.leaseId,
      path: lease.path,
      repoRoot: lease.repoRoot,
      boundAt: Date.now(),
    })
    return this.store.provisionOperator(request)
  }

  /**
   * Run one claimed intent to completion. Activations of the same operator are
   * serialized (one child run at a time). A child run never fails the
   * activation: its terminal summary is emitted through `recordSink` (and
   * returned as records); only genuine programming errors propagate.
   */
  async runClaimedAgentGraphIntent(
    input: AgentGraphRunClaimedIntentInput,
  ): Promise<AgentGraphRecord[]> {
    const operatorId = input.intent.operatorId
    const previous = this.operatorTails.get(operatorId) ?? Promise.resolve()
    const queued = previous.then(
      () => this.runClaimedOnce(input),
      () => this.runClaimedOnce(input),
    )
    const tail = queued.then(
      () => undefined,
      () => undefined,
    )
    this.operatorTails.set(operatorId, tail)
    try {
      return await queued
    } finally {
      if (this.operatorTails.get(operatorId) === tail) this.operatorTails.delete(operatorId)
    }
  }

  /** Stop a running operator child (graph-supervisor stops are batched by the driver). */
  async stopSession(
    sessionId: string,
    opts?: { source?: 'graph_supervisor' },
  ): Promise<void> {
    const reason = opts?.source
    if (reason !== undefined) {
      await this.childRunner.stop(sessionId, { reason })
    } else {
      await this.childRunner.stop(sessionId)
    }
  }

  private async acquireLease(
    request: AgentGraphOperatorProvisionRequest,
  ): Promise<GraphOperatorWorktreeLease | undefined> {
    try {
      return await this.pool.acquire(provisionKey(request))
    } catch {
      // Pool exhaustion or a transient git failure defers this operator; the
      // deterministic key makes the next attempt adopt the same lease.
      return undefined
    }
  }

  private async runClaimedOnce(
    input: AgentGraphRunClaimedIntentInput,
  ): Promise<AgentGraphRecord[]> {
    if (input.abortSignal?.aborted === true) return []
    if (input.admitExecution !== undefined) {
      const state = await input.admitExecution()
      if (state === 'cancelled') return []
    }
    const binding = await this.bindingFor(input)
    const start: GraphOperatorChildStartInput = {
      sessionId: input.intent.targetSessionId,
      instructions: input.prompt,
      workspace: binding.path,
      runId: input.claim.targetRunId,
      labels: {
        graphId: input.intent.graphId,
        operatorId: input.intent.operatorId,
        workId: input.intent.readinessId,
      },
      ...(input.abortSignal !== undefined ? { abortSignal: input.abortSignal } : {}),
    }
    const run = await this.startChild(start)
    return this.settle(input, run)
  }

  private async startChild(input: GraphOperatorChildStartInput): Promise<AgentGraphChildRun> {
    if (this.limiter === undefined) return this.childRunner.start(input)
    return this.limiter.run(() => this.childRunner.start(input))
  }

  /**
   * Resolve the worktree binding of this activation. The intent's readiness id
   * names the provisioning work for dynamic operators (indexed read);
   * operator-targeted work re-runs a provisioned operator, so the fallback
   * finds the provision through its `operatorId`.
   */
  private async bindingFor(input: AgentGraphRunClaimedIntentInput): Promise<AgentGraphOperatorBinding> {
    const graphId = input.intent.graphId
    const operatorId = input.intent.operatorId
    const byWork = await this.store.readOperatorBindingByWork(graphId, input.intent.readinessId)
    if (byWork !== undefined) return byWork
    let provision: AgentGraphOperatorProvision | undefined
    for (const candidate of await this.store.listOperatorProvisions(graphId)) {
      if (candidate.operatorId === operatorId) {
        provision = candidate
        break
      }
    }
    if (provision === undefined) {
      throw new Error(`agent graph ${graphId}: operator ${operatorId} has no provision to run`)
    }
    const binding = await this.store.readOperatorBinding(provision.provisionId)
    if (binding === undefined) {
      throw new Error(`agent graph ${graphId}: operator ${operatorId} has no worktree binding`)
    }
    return binding
  }

  /** Emit one terminal source event and fold it into the returned record. */
  private async settle(
    input: AgentGraphRunClaimedIntentInput,
    run: AgentGraphChildRun,
  ): Promise<AgentGraphRecord[]> {
    const event: AgentGraphRecordSourceEvent = {
      runtimeEventId: this.newId(),
      seq: 1,
      runId: input.claim.targetRunId,
      summary: truncateUtf8(terminalSummary(run), TERMINAL_SUMMARY_MAX_BYTES),
      terminal: true,
      partial: false,
      facets: ['message', 'terminal'],
      emittedAt: Date.now(),
    }
    if (this.recordSink !== undefined) await this.recordSink(event)
    return [recordFor(input, event)]
  }
}

/** Terminal summary of one settled child run; failure detail is prefixed. */
function terminalSummary(run: AgentGraphChildRun): string {
  if (run.summary !== undefined && run.summary.length > 0) return run.summary
  if (run.outcome === 'failed') return `[operator failed] ${errorMessage(run.error)}`
  if (run.outcome === 'cancelled') return '[operator cancelled]'
  return '[operator fulfilled]'
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message
  return 'unknown error'
}

function recordFor(
  input: AgentGraphRunClaimedIntentInput,
  event: AgentGraphRecordSourceEvent,
): AgentGraphRecord {
  return {
    recordId: graphRecordId(
      input.intent.graphId,
      input.intent.operatorId,
      input.intent.targetSessionId,
      event.runId,
      event.runtimeEventId,
    ),
    graphId: input.intent.graphId,
    operatorId: input.intent.operatorId,
    source: {
      sessionId: input.intent.targetSessionId,
      runId: event.runId,
      runtimeEventId: event.runtimeEventId,
      seq: event.seq,
    },
    summary: event.summary,
    facets: event.facets ?? [],
    emittedAt: event.emittedAt,
  }
}

/** Counting semaphore capping concurrently running child starts. */
class ConcurrencyLimiter {
  private active = 0
  private readonly waiters: (() => void)[] = []

  constructor(private readonly max: number) { }

  async run<T>(task: () => Promise<T>): Promise<T> {
    await this.acquire()
    try {
      return await task()
    } finally {
      this.release()
    }
  }

  private async acquire(): Promise<void> {
    if (this.active < this.max) {
      this.active += 1
      return
    }
    await new Promise<void>((resolve) => {
      this.waiters.push(resolve)
    })
    this.active += 1
  }

  private release(): void {
    this.active -= 1
    const next = this.waiters.shift()
    if (next !== undefined) next()
  }
}

function validateConcurrencyHint(hint: number): ConcurrencyLimiter {
  if (!Number.isSafeInteger(hint) || hint < 1) {
    throw new Error(
      'agent graph executor concurrencyHint must be a positive safe integer',
    )
  }
  return new ConcurrencyLimiter(hint)
}

/** Create one executor over the injected seams (thin factory for the host wiring). */
export function createGraphOperatorExecutor(
  options: AgentGraphOperatorExecutorOptions,
): AgentGraphOperatorExecutor {
  return new AgentGraphOperatorExecutor(options)
}
