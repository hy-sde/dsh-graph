/**
 * Host-side wake delivery runtime (Maka `AgentGraphSupervisorWakeCoordinator`,
 * port subset). Delivers due supervisor wakes only at root-session idle
 * boundaries, never interrupting a running turn, and settles every attempt in
 * the durable store through the same begin/complete CAS the P1 store provides.
 * @module @hy-sde-org/dsh-graph-wakes
 */

import { graphWakeAttemptId } from '@hy-sde-org/dsh-graph-control'
import type {
  AgentGraphScheduleUpdate,
  AgentGraphSupervisorWakeRecord,
  CompleteAgentGraphSupervisorWakeAttemptRequest,
} from '@hy-sde-org/dsh-graph-control'
import type {
  GraphWakeDeliver,
  GraphWakeDeliveryOutcome,
  GraphWakeIdleCallback,
  GraphWakeRuntimeOptions,
  GraphWakeStore,
} from './types.ts'

/** Largest delay that Node timers represent without clamping. */
export const MAX_TIMER_DELAY_MS = 2_147_483_647

/** Default ceiling on delivery attempts per wake (Maka default: 3). */
export const DEFAULT_MAX_DELIVERY_ATTEMPTS = 3

/** Default re-arm base for a retryable failure: 30 s times the attempt number. */
export const DEFAULT_RETRY_BACKOFF_MS = 30_000

/** One process-local re-arm: when a retryable wake may be attempted again. */
interface RearmEntry {
  readonly at: number
  readonly rootSessionId: string
}

/** Per-wake context-overflow recovery state (process-local only). */
interface OverflowRecovery {
  compactAttempted: boolean
  partialAttempted: boolean
  /** No further delivery is allowed in this process (partial ran or no recovery). */
  exhausted: boolean
}

function renderError(value: unknown): string {
  return value instanceof Error ? value.message : String(value)
}

/**
 * Deliver permanent delivery statuses and map the runtime-only `stopped`
 * outcome onto the store's `superseded` status.
 */
function completeStatus(kind: GraphWakeDeliveryOutcome['kind']): CompleteAgentGraphSupervisorWakeAttemptRequest['status'] {
  switch (kind) {
    case 'delivered':
      return 'delivered'
    case 'waiting_permission':
      return 'waiting_permission'
    case 'superseded':
      return 'superseded'
    case 'stopped':
      return 'superseded'
    case 'retryable_failed':
      return 'retryable_failed'
  }
}

/**
 * One process-local runtime that carries due supervisor wakes to the owning
 * root session at its next idle boundary.
 *
 * The runtime only ever starts a delivery from the idle seam
 * ({@link handleIdle}, normally reached through the injected
 * {@link GraphWakeRuntimeOptions.observeIdle}), mirroring the Schedule
 * package's status listener. It never touches a running turn: the deliver hook
 * (host wiring) is responsible for running the wake through the owning agent's
 * maintenance seam once idle. Every transition is store-sealed — the attempt
 * CAS is `beginSupervisorWakeAttempt` with a deterministic attempt id, so
 * overlapping sweep invocations deliver at most once per attempt row.
 */
export class GraphWakeRuntime {
  private readonly store: GraphWakeStore
  private readonly deliver: GraphWakeDeliver
  private readonly onCompact: ((sessionId: string) => Promise<void>) | undefined
  private readonly now: () => number
  private readonly maxAttempts: number
  private readonly observeIdle: ((onIdle: GraphWakeIdleCallback) => () => void) | undefined
  private readonly onError: ((sessionId: string, error: unknown) => void) | undefined

  private started = false
  private stopping = false
  private rootSessionId: string | undefined
  private unsubscribeIdle: (() => void) | undefined
  private sweep: Promise<void> | undefined
  private sweepRequested = false
  private idleSession: string | undefined
  private timer: ReturnType<typeof setTimeout> | undefined
  private readonly rearm = new Map<string, RearmEntry>()
  private readonly overflowRecovery = new Map<string, OverflowRecovery>()

  /**
   * Construct an inactive runtime; {@link start} subscribes the idle observer.
   * @param options - store seam, deliver hook, and optional recovery/timing knobs.
   * @throws when `maxAttempts` is not a positive safe integer.
   */
  constructor(options: GraphWakeRuntimeOptions) {
    this.store = options.store
    this.deliver = options.deliver
    this.onCompact = options.onCompact
    this.now = options.now ?? (() => Date.now())
    this.maxAttempts = options.maxAttempts ?? DEFAULT_MAX_DELIVERY_ATTEMPTS
    this.observeIdle = options.observeIdle
    this.onError = options.onError
    if (!Number.isSafeInteger(this.maxAttempts) || this.maxAttempts < 1) {
      throw new Error('agent graph wakes: delivery attempts must be a positive safe integer')
    }
  }

  /**
   * Subscribe the idle observer and arm re-drive timers for process-local
   * re-arms. `start` never delivers on its own: delivery waits for an idle
   * signal (host wiring observes `agent/status === 'idle'` on live roots).
   *
   * @param rootSessionId - optional scope: only this root's wakes are observed.
   * @throws when the runtime is already started or stopped.
   */
  start(rootSessionId?: string): void {
    if (this.started) throw new Error('agent graph wakes: runtime already started')
    if (this.stopping) throw new Error('agent graph wakes: runtime is stopped')
    this.started = true
    this.rootSessionId = rootSessionId
    this.unsubscribeIdle = this.observeIdle?.((sessionId) => {
      if (rootSessionId !== undefined && sessionId !== rootSessionId) return
      void this.handleIdle(sessionId)
    })
  }

  /**
   * Stop future deliveries, unsubscribe the observer, cancel timers, and await
   * the in-flight sweep (its attempt outcome is still settled durably).
   * @returns a promise resolving when the in-flight sweep, if any, completes.
   */
  stop(): Promise<void> {
    this.stopping = true
    this.sweepRequested = false
    this.idleSession = undefined
    this.unsubscribeIdle?.()
    this.unsubscribeIdle = undefined
    this.clearTimer()
    this.rearm.clear()
    return this.sweep ?? Promise.resolve()
  }

  /**
   * Enter one idle boundary: coalesces an in-flight sweep and returns the
   * sweep promise. This is the single delivery entry point — the injected
   * observer forwards to it, tests drive it directly.
   *
   * @param sessionId - scope the sweep to one root session; omit for all.
   * @returns a promise resolving when the triggered sweep settles.
   */
  handleIdle(sessionId?: string): Promise<void> {
    this.idleSession = sessionId
    this.requestSweep()
    return this.sweep ?? Promise.resolve()
  }

  /**
   * Wakes still open to the runtime: pending and retryable (terminal exhausted
   * wakes included), optionally for one graph. Running, parked, delivered, and
   * superseded wakes are excluded.
   * @param graphId - optional graph filter.
   * @returns a copy of the matching wakes, ordered by creation time.
   */
  async pendingWakes(graphId?: string): Promise<AgentGraphSupervisorWakeRecord[]> {
    const wakes = await this.store.listUnsettledSupervisorWakes()
    return wakes
      .filter(wake =>
        (graphId === undefined || wake.graphId === graphId) &&
        (wake.status === 'pending' || wake.status === 'retryable_failed'))
      .sort((left, right) => left.createdAt - right.createdAt)
  }

  /** Durable status of one wake (any terminal or unsettled status). */
  wakeStatus(wakeId: string): Promise<AgentGraphSupervisorWakeRecord | undefined> {
    return this.store.snapshot().then(snapshot => snapshot.supervisorWakes.find(wake => wake.wakeId === wakeId))
  }

  /** Coalesce one sweep run; the run loop drains every request queued during it. */
  private requestSweep(): void {
    if (this.stopping) return
    this.sweepRequested = true
    if (this.sweep !== undefined) return
    const run = this.driveSweeps()
    this.sweep = run
    void run.then(
      () => { this.retire(run) },
      () => { this.retire(run) },
    )
  }

  /** Retire one exact sweep and honor a request that landed in its final microtask. */
  private retire(run: Promise<void>): void {
    if (this.sweep !== run) return
    this.sweep = undefined
    if (this.sweepRequested && !this.stopping) this.requestSweep()
  }

  /** Drain coalesced idle triggers serially; never rejects. */
  private async driveSweeps(): Promise<void> {
    while (this.sweepRequested && !this.stopping) {
      this.sweepRequested = false
      try {
        await this.sweepOnce()
      } catch (error: unknown) {
        this.report('', error)
      }
    }
  }

  /** One sweep: enumerate due wakes for the idle scope and deliver each serially. */
  private async sweepOnce(): Promise<void> {
    const scope = this.idleSession
    this.idleSession = undefined
    let wakes: AgentGraphSupervisorWakeRecord[]
    try {
      wakes = await this.store.listUnsettledSupervisorWakes()
    } catch (error: unknown) {
      this.report(scope ?? '', error)
      return
    }
    if (this.stopping) return
    const due = wakes
      .filter(wake => this.isDue(wake, scope))
      .sort((left, right) => left.createdAt - right.createdAt || compareIdentity(left.wakeId, right.wakeId))
    for (const wake of due) {
      try {
        await this.deliverWake(wake)
      } catch (error: unknown) {
        this.report(wake.rootSessionId, error)
      }
      if (this.isStopping()) return
    }
  }

  /** Whether a stop request landed; kept as a guard for after-await checkpoints. */
  private isStopping(): boolean {
    return this.stopping
  }

  /** Whether one unsettled wake is due in the current sweep scope. */
  private isDue(wake: AgentGraphSupervisorWakeRecord, sessionScope: string | undefined): boolean {
    if (sessionScope !== undefined && wake.rootSessionId !== sessionScope) return false
    if (this.rootSessionId !== undefined && wake.rootSessionId !== this.rootSessionId) return false
    if (wake.status === 'pending') return true
    if (wake.status !== 'retryable_failed') return false
    if (wake.attemptCount >= this.maxAttempts) return false
    if (this.overflowRecovery.get(wake.wakeId)?.exhausted === true) return false
    const entry = this.rearm.get(wake.wakeId)
    return entry === undefined || entry.at <= this.now()
  }

  /** Deliver one wake: stop-check, store CAS, deliver hook, durable settle. */
  private async deliverWake(wake: AgentGraphSupervisorWakeRecord): Promise<void> {
    if (await this.isGraphStopped(wake)) {
      await this.store.supersedeSupervisorWakes({
        rootSessionIds: [wake.rootSessionId],
        graphIds: [wake.graphId],
        reason: 'agent_graph_stopped',
      })
      this.forget(wake.wakeId)
      return
    }
    const attemptIndex = wake.attemptCount + 1
    const attemptId = graphWakeAttemptId(wake.wakeId, attemptIndex)
    const begun = await this.store.beginSupervisorWakeAttempt({
      graphId: wake.graphId,
      wakeId: wake.wakeId,
      attemptId,
      turnId: attemptId,
    })
    // Another runtime or a retried sweep already owns this attempt row.
    if (!begun.acquired || begun.attempt === undefined) return
    let outcome: GraphWakeDeliveryOutcome
    try {
      outcome = await this.deliver({
        graphId: wake.graphId,
        wakeId: wake.wakeId,
        rootSessionId: wake.rootSessionId,
        snapshotVersion: wake.snapshotVersion,
      })
    } catch (error: unknown) {
      outcome = { kind: 'retryable_failed', failureReason: renderError(error) }
    }
    await this.settle(wake, begun.attempt.attemptId, outcome)
  }

  /** Persist one attempt outcome and re-arm (or terminate) a retryable wake. */
  private async settle(
    wake: AgentGraphSupervisorWakeRecord,
    attemptId: string,
    outcome: GraphWakeDeliveryOutcome,
  ): Promise<void> {
    const status = completeStatus(outcome.kind)
    const reason = this.failureReason(outcome, status)
    await this.store.completeSupervisorWakeAttempt({
      graphId: wake.graphId,
      wakeId: wake.wakeId,
      attemptId,
      status,
      ...(reason !== undefined ? { failureReason: reason } : {}),
    })
    if (status !== 'retryable_failed') {
      this.forget(wake.wakeId)
      return
    }
    const attemptCount = wake.attemptCount + 1
    if (attemptCount >= this.maxAttempts) {
      this.forget(wake.wakeId)
      return
    }
    if (outcome.overflow === true) {
      await this.recoverOverflow(wake, outcome)
      return
    }
    const at = outcome.nextAttemptAt ?? this.now() + DEFAULT_RETRY_BACKOFF_MS * attemptCount
    this.rearm.set(wake.wakeId, { at, rootSessionId: wake.rootSessionId })
    this.armTimer()
  }

  /** One-compact-then-one-partial recovery; never a third identical full retry. */
  private async recoverOverflow(
    wake: AgentGraphSupervisorWakeRecord,
    outcome: GraphWakeDeliveryOutcome,
  ): Promise<void> {
    const previous = this.overflowRecovery.get(wake.wakeId) ?? {
      compactAttempted: false,
      partialAttempted: false,
      exhausted: false,
    }
    const partialAttempted = previous.partialAttempted || outcome.partialResult === true
    // The bounded partial already ran, this overflow was expected to be the
    // partial but did not declare itself, or no recovery is wired: terminal.
    if (partialAttempted || previous.compactAttempted || this.onCompact === undefined) {
      this.overflowRecovery.set(wake.wakeId, {
        compactAttempted: previous.compactAttempted,
        partialAttempted,
        exhausted: true,
      })
      this.rearm.delete(wake.wakeId)
      this.armTimer()
      return
    }
    const state: OverflowRecovery = { compactAttempted: true, partialAttempted: false, exhausted: false }
    this.overflowRecovery.set(wake.wakeId, state)
    try {
      await this.onCompact(wake.rootSessionId)
    } catch (error: unknown) {
      this.report(wake.rootSessionId, error)
      this.overflowRecovery.set(wake.wakeId, { ...state, exhausted: true })
      this.rearm.delete(wake.wakeId)
      this.armTimer()
      return
    }
    this.rearm.set(wake.wakeId, { at: this.now(), rootSessionId: wake.rootSessionId })
    this.armTimer()
  }

  /**
   * Whether the graph is stopped from the runtime's point of view: a schedule
   * log `finish` (graph closed) or a log stop whose target id is the root
   * session or the graph id (the graph-level stop convention; work-item stops
   * target work ids and never cancel wakes). The coordinator's own stop
   * lifecycle stays host wiring; the durable log is the shared authority.
   */
  private async isGraphStopped(wake: AgentGraphSupervisorWakeRecord): Promise<boolean> {
    let updates: AgentGraphScheduleUpdate[]
    try {
      updates = await this.store.listScheduleUpdates(wake.graphId)
    } catch (error: unknown) {
      this.report(wake.rootSessionId, error)
      return false
    }
    if (updates.some(update => update.finish !== undefined)) return true
    return updates.some(update =>
      update.stop.some(stopped =>
        stopped.targetId === wake.rootSessionId || stopped.targetId === wake.graphId))
  }

  /** Arm one bounded timer segment toward the next re-armed wake; every fire re-checks. */
  private armTimer(): void {
    this.clearTimer()
    if (this.stopping) return
    const now = this.now()
    let earliest: RearmEntry | undefined
    for (const entry of this.rearm.values()) {
      if (earliest === undefined || entry.at < earliest.at) earliest = entry
    }
    if (earliest === undefined) return
    const delay = Math.min(Math.max(0, earliest.at - now), MAX_TIMER_DELAY_MS)
    this.timer = setTimeout(() => {
      this.timer = undefined
      void this.handleIdle(earliest.rootSessionId)
    }, delay)
  }

  /** Drop per-wake process state and re-evaluate the re-drive timer. */
  private forget(wakeId: string): void {
    this.rearm.delete(wakeId)
    this.overflowRecovery.delete(wakeId)
    this.armTimer()
  }

  private clearTimer(): void {
    if (this.timer === undefined) return
    clearTimeout(this.timer)
    this.timer = undefined
  }

  private failureReason(
    outcome: GraphWakeDeliveryOutcome,
    status: CompleteAgentGraphSupervisorWakeAttemptRequest['status'],
  ): string | undefined {
    if (outcome.failureReason !== undefined) return outcome.failureReason
    if (status === 'retryable_failed') return 'delivery_failed'
    if (outcome.kind === 'stopped') return 'agent_graph_stopped'
    return undefined
  }

  private report(sessionId: string, error: unknown): void {
    try {
      this.onError?.(sessionId, error)
    } catch {
      // Delivery correctness never depends on an error observer.
    }
  }
}

function compareIdentity(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0
}
