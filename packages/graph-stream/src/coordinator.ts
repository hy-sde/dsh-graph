/**
 * Process-local single-flight coordinator (Maka `AgentGraphCoordinator`,
 * port subset: driver loop only). The store is the authority — this class
 * only (re)reads rows, notifies observers, and re-requests drives while work
 * remains. `scheduleUpdate` commits a schedule row then wakes the drive;
 * `reconcileAndWait` runs exactly one drive to idle; `recover` resumes graphs
 * found with non-empty schedule after a restart.
 * @module
 */

import {
  type AgentGraphScheduleUpdateRequest,
  type AgentGraphScheduleUpdateResult,
  type AgentGraphScheduledWork,
} from '@hy-sde-org/dsh-graph-control'
import { stableHash32 } from './hash.ts'
import type { AgentGraphRecordSource } from './projection.ts'
import {
  reconcileAgentGraphSchedule,
  type AgentGraphReconcileSeams,
  type AgentGraphScheduleReconciliationResult,
} from './reconcile.ts'
import type { AgentGraphInputHandoff } from './handoff.ts'
import type {
  AgentGraphExecutor,
  AgentGraphRecord,
  AgentGraphSupervisorObservation,
  AgentGraphReconciliationTopology,
} from './types.ts'
export interface AgentGraphCoordinatorOptions {
  readonly graphId: string
  readonly store: AgentGraphReconcileSeams['store'] & {
    commitScheduleUpdate(
      request: AgentGraphScheduleUpdateRequest,
    ): Promise<AgentGraphScheduleUpdateResult>
  }
  readonly executor: AgentGraphExecutor
  readonly recordSource: AgentGraphRecordSource
  readonly newId: () => string
  readonly maxNewActivations: number
  readonly observeGraph?: (
    topology: AgentGraphReconciliationTopology,
  ) => Promise<AgentGraphSupervisorObservation>
  readonly resolveSelectedResultInputs?: AgentGraphReconcileSeams['resolveSelectedResultInputs']
  readonly hydrateInputHandoffs?: (
    records: readonly AgentGraphRecord[],
  ) => Promise<AgentGraphInputHandoff[]>
  readonly renderPrompt?: (input: {
    work: AgentGraphScheduledWork
    inputRecords: readonly AgentGraphRecord[]
    inputHandoffs: readonly AgentGraphInputHandoff[]
  }) => string
  readonly onReconciliation?: (
    result: AgentGraphScheduleReconciliationResult,
  ) => void
  readonly onError?: (error: unknown) => void
  readonly onScheduleCommitted?: (
    result: AgentGraphScheduleUpdateResult,
  ) => void
}

/**
 * Drives one graph: commits schedule updates, reconciles to idle, never
 * interrupts a running turn of the host, survives restarts (recover).
 */
export class AgentGraphCoordinator {
  readonly graphId: string

  private readonly options: AgentGraphCoordinatorOptions
  private requested = false
  private paused = false
  private stopping = false
  private stopGeneration = 0
  private task: Promise<void> | undefined
  private closed = false
  private abortController: AbortController | undefined
  private lastResult: AgentGraphScheduleReconciliationResult | undefined
  private lastError: unknown

  constructor(options: AgentGraphCoordinatorOptions) {
    this.graphId = options.graphId
    this.options = options
  }

  /** Commits one schedule update then wakes the drive; returns the store result. */
  async scheduleUpdate(
    request: AgentGraphScheduleUpdateRequest,
  ): Promise<AgentGraphScheduleUpdateResult> {
    this.assertOpen()
    const result = await this.options.store.commitScheduleUpdate(request)
    this.options.onScheduleCommitted?.(result)
    this.#requestDrive()
    return result
  }

  /** Runs one drive to idle (single-flight; joins an in-flight drive). */
  async reconcileAndWait(): Promise<AgentGraphScheduleReconciliationResult> {
    this.assertOpen()
    this.#requestDrive()
    await this.#driveTask()
    if (this.lastError !== undefined) {
      throw this.lastError instanceof Error
        ? this.lastError
        : new Error(`agent graph ${this.graphId}: reconciliation failed`, { cause: this.lastError })
    }
    if (this.lastResult !== undefined) return this.lastResult
    throw new Error(
      `agent graph ${this.graphId}: no reconciliation result (drive did not run)`,
    )
  }

  /** Recover after restart: reconcile whatever schedule is durable. */
  async recover(): Promise<AgentGraphScheduleReconciliationResult> {
    const updates = await this.options.store.listScheduleUpdates(this.graphId)
    if (updates.length === 0) return emptyResult(this.graphId)
    return this.reconcileAndWait()
  }

  /** Stops the drive: aborts in-flight reconcile and refuses further drives until `wake()`. */
  stop(): void {
    this.stopGeneration += 1
    this.paused = true
    this.stopping = true
    this.requested = false
    this.abortController?.abort()
  }

  /** Re-enables a stopped drive (wake). */
  wake(): void {
    if (this.stopping) this.stopping = false
    this.paused = false
    this.#requestDrive()
  }

  isClosed(): boolean {
    return this.closed
  }

  private assertOpen(): void {
    if (this.closed)
      throw new Error(`agent graph ${this.graphId}: coordinator is closed`)
  }

  #requestDrive(): void {
    this.requested = true
    if (this.task !== undefined) return
    if (this.paused || this.stopping || this.closed) return
    this.task = this.#drive()
      .catch(() => { })
      .finally(() => {
        this.task = undefined
        if (this.requested) this.#requestDrive()
      })
  }

  #driveTask(): Promise<void> {
    return this.task ?? Promise.resolve()
  }

  async #drive(): Promise<void> {
    while (this.requested && !this.paused && !this.stopping && !this.closed) {
      this.requested = false
      this.lastError = undefined
      const abortController = new AbortController()
      this.abortController = abortController
      try {
        const result = await this.#reconcileOnce(abortController.signal)
        this.lastResult = result
        this.options.onReconciliation?.(result)
      } catch (error) {
        if (!abortController.signal.aborted) {
          this.lastError = error
          this.options.onError?.(error)
        }
      } finally {
        this.abortController = undefined
      }
    }
  }

  async #reconcileOnce(
    abortSignal: AbortSignal,
  ): Promise<AgentGraphScheduleReconciliationResult> {
    return reconcileAgentGraphSchedule({
      graphId: this.graphId,
      store: this.options.store,
      executor: this.options.executor,
      recordSource: this.options.recordSource,
      observeGraph:
        this.options.observeGraph ??
        ((topology: AgentGraphReconciliationTopology) =>
          observeFromRecords(topology, this.options.recordSource)),
      newId: this.options.newId,
      maxNewActivations: this.options.maxNewActivations,
      ...(this.options.resolveSelectedResultInputs !== undefined
        ? { resolveSelectedResultInputs: this.options.resolveSelectedResultInputs }
        : {}),
      ...(this.options.hydrateInputHandoffs !== undefined
        ? { hydrateInputHandoffs: this.options.hydrateInputHandoffs }
        : {}),
      renderPrompt: this.options.renderPrompt ?? defaultRenderPrompt,
      abortSignal,
    })
  }
}

/** Default observation: records folded from the record source (no readiness in P2). */
async function observeFromRecords(
  topology: AgentGraphReconciliationTopology,
  recordSource: AgentGraphRecordSource,
): Promise<AgentGraphSupervisorObservation> {
  const records: AgentGraphRecord[] = []
  const operators: {
    operatorId: string
    sessionId: string
    terminal: boolean
  }[] = []
  for (const operator of topology.operators) {
    const events = await recordSource.listCommittedEvents(
      operator.operatorId,
      operator.sessionId,
    )
    const runIds = [...new Set(events.map(event => event.runId))]
    const terminalByRun = new Set<string>()
    let terminal = false
    for (const runId of runIds) {
      const runEvents = events
        .filter(event => event.runId === runId)
        .sort((a, b) => a.seq - b.seq)
      for (const event of runEvents) {
        if (event.partial === true || terminalByRun.has(runId)) continue
        if (event.terminal) {
          terminalByRun.add(runId)
          terminal = true
        }
        records.push({
          recordId: graphRecordIdLite(
            topology.graphId,
            operator.operatorId,
            operator.sessionId,
            runId,
            event.runtimeEventId,
          ),
          graphId: topology.graphId,
          operatorId: operator.operatorId,
          source: {
            sessionId: operator.sessionId,
            runId,
            runtimeEventId: event.runtimeEventId,
            seq: event.seq,
          },
          summary: event.summary,
          facets: event.terminal ? ['message', 'terminal'] : ['message'],
          emittedAt: event.emittedAt,
        })
      }
    }
    operators.push({
      operatorId: operator.operatorId,
      sessionId: operator.sessionId,
      terminal,
    })
  }
  return { graphId: topology.graphId, records, operators }
}

function graphRecordIdLite(
  graphId: string,
  operatorId: string,
  sessionId: string,
  runId: string,
  runtimeEventId: string,
): string {
  return `graph_record_${stableHash32({ graphId, operatorId, sessionId, runId, runtimeEventId })}`
}

function defaultRenderPrompt(input: {
  work: AgentGraphScheduledWork
  inputHandoffs: readonly AgentGraphInputHandoff[]
}): string {
  const sections: string[] = [input.work.instruction]
  if (input.inputHandoffs.length > 0) {
    sections.push(
      `<agent_graph_input_handoffs encoding="json">\n${JSON.stringify(input.inputHandoffs, null, 2)}\n</agent_graph_input_handoffs>`,
    )
  }
  return sections.join('\n\n')
}

function emptyResult(graphId: string): AgentGraphScheduleReconciliationResult {
  return {
    status: 'reconciled',
    scheduledCandidateCount: 0,
    dispatched: 0,
    failures: [],
    deferred: [],
    schedule: {
      schemaVersion: 1,
      graphId,
      closed: false,
      revision: 0,
      updateCount: 0,
      work: [],
      stoppedTargets: [],
    },
    newActivationCount: 0,
    observedExistingActivationCount: 0,
    dispatches: [],
    stops: [],
    deferredWork: [],
  }
}
