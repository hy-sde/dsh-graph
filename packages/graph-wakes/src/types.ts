/**
 * Contract types for Agent Graph wake delivery: the runtime options, the
 * deliver-hook outcome union, and the store seam the runtime reads/writes
 * (a structural subset of {@link GraphControlStore}).
 * @module @hy-sde-org/dsh-graph-wakes
 */

import type {
  AgentGraphControlSnapshot,
  AgentGraphScheduleUpdate,
  AgentGraphSupervisorWakeAttemptRecord,
  AgentGraphSupervisorWakeRecord,
  BeginAgentGraphSupervisorWakeAttemptRequest,
  CompleteAgentGraphSupervisorWakeAttemptRequest,
  SupersedeAgentGraphSupervisorWakesRequest,
} from '@hy-sde-org/dsh-graph-control'

/** What one delivery attempt asks to enter the owning root supervisor's turn. */
export interface GraphWakeDue {
  readonly graphId: string
  readonly wakeId: string
  readonly rootSessionId: string
  /** Wake snapshot identity the durable row was claimed for. */
  readonly snapshotVersion: string
}

/**
 * Result of one attempted wake delivery. The runtime maps each kind onto the
 * durable attempt status (`delivered` | `waiting_permission` | `superseded` |
 * `retryable_failed`); `stopped` settles as `superseded` because the store's
 * wake status union has no `stopped` member (Maka's `AgentGraphSupervisorWakeStatus`).
 */
export interface GraphWakeDeliveryOutcome {
  readonly kind: 'delivered' | 'waiting_permission' | 'retryable_failed' | 'superseded' | 'stopped'
  /** Provider-confirmed context overflow; triggers the one-compact recovery path. */
  readonly overflow?: boolean
  /** True when this attempt carried the bounded partial result after recovery. */
  readonly partialResult?: boolean
  /** Absolute re-arm time (ms) that overrides the default 30 s × attempt backoff. */
  readonly nextAttemptAt?: number
  /** Human-readable failure retained on the attempt row for `retryable_failed`. */
  readonly failureReason?: string
}

/** One delivery attempt into the owning root session (host wiring supplies it). */
export type GraphWakeDeliver = (due: GraphWakeDue) => Promise<GraphWakeDeliveryOutcome>

/** Called with a root session id whenever the owning agent reports idle. */
export type GraphWakeIdleCallback = (sessionId: string) => void

/** Subscribes to root-session idle boundaries; returns the unsubscriber. */
export type GraphWakeIdleObserver = (onIdle: GraphWakeIdleCallback) => () => void

/**
 * Structural store seam: the subset of {@link GraphControlStore} the runtime
 * uses. `GraphControlStore` satisfies it exactly.
 */
export interface GraphWakeStore {
  listUnsettledSupervisorWakes(): Promise<AgentGraphSupervisorWakeRecord[]>
  readSupervisorWake(graphId: string, wakeId: string): Promise<AgentGraphSupervisorWakeRecord | undefined>
  beginSupervisorWakeAttempt(request: BeginAgentGraphSupervisorWakeAttemptRequest): Promise<{
    wake: AgentGraphSupervisorWakeRecord
    attempt?: AgentGraphSupervisorWakeAttemptRecord
    acquired: boolean
  }>
  completeSupervisorWakeAttempt(
    request: CompleteAgentGraphSupervisorWakeAttemptRequest,
  ): Promise<AgentGraphSupervisorWakeRecord>
  supersedeSupervisorWakes(request: SupersedeAgentGraphSupervisorWakesRequest): Promise<number>
  listScheduleUpdates(graphId: string): Promise<AgentGraphScheduleUpdate[]>
  snapshot(): Promise<AgentGraphControlSnapshot>
}

/** Construction options for {@link GraphWakeRuntime}. */
export interface GraphWakeRuntimeOptions {
  readonly store: GraphWakeStore
  readonly deliver: GraphWakeDeliver
  /**
   * One bounded compaction of the root session after a context overflow; the
   * runtime calls it at most once per wake.
   */
  readonly onCompact?: (sessionId: string) => Promise<void>
  /** Wall clock in ms; overridable for deterministic tests. */
  readonly now?: () => number
  /** Delivery-attempt ceiling per wake; defaults to 3. */
  readonly maxAttempts?: number
  /**
   * Idle-boundary subscription. The runtime never starts a delivery without an
   * idle signal through {@link GraphWakeRuntime.handleIdle}; production wiring
   * passes a cordis observer over `agent/status === 'idle'` for live root
   * agents (the Schedule package's seam), tests pass a fake observer.
   */
  readonly observeIdle?: GraphWakeIdleObserver
  /** Observes delivery/store failures; observers never alter delivery correctness. */
  readonly onError?: (sessionId: string, error: unknown) => void
}
