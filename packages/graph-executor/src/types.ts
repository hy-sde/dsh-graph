/**
 * Testable seams and option types of the Agent Graph operator executor
 * (Maka port, slice P3): the child subagent runner and the worktree pool are
 * interfaces so the executor logic is unit-testable without a real runtime or
 * git, while the host wiring supplies subagent-provider and worktree-pool
 * implementations.
 * @module
 */

import type { AgentGraphRecordSourceEvent } from '@hy-sde-org/dsh-graph-stream'
import type { GraphControlStore } from '@hy-sde-org/dsh-graph-control'

/** Outcome of one child run as reported by the runner. */
export interface AgentGraphChildRun {
  readonly outcome: 'fulfilled' | 'failed' | 'cancelled'
  /** Final output text, when the runner produced any. */
  readonly summary?: string
  /** Failure detail, present when `outcome` is `failed`. */
  readonly error?: unknown
}

/** One child start request the executor issues. */
export interface GraphOperatorChildStartInput {
  /** The operator's child session the graph provisioned. */
  readonly sessionId: string
  /** The model-facing prompt rendered for this activation. */
  readonly instructions: string
  /** Absolute workspace path of the operator's leased worktree, when bound. */
  readonly workspace?: string
  readonly parentSessionId?: string
  /** The activation's run identity (claim `targetRunId`). */
  readonly runId?: string
  /** Host-consumable labels (graph, operator, work provenance). */
  readonly labels?: Record<string, string>
  /** Cancellation signal from the graph driver. */
  readonly abortSignal?: AbortSignal
}

/**
 * Testable subagent seam: one provider-backed one-shot child run per start,
 * scoped to an existing child session. `start` settles when the child run is
 * terminal; `stop` requests a running child to stop.
 */
export interface GraphOperatorChildRunner {
  start(input: GraphOperatorChildStartInput): Promise<AgentGraphChildRun>
  stop(sessionId: string, opts?: { reason?: string }): Promise<void>
}

/** One leased worktree as the executor's pool exposes it. */
export interface GraphOperatorWorktreeLease {
  /** Immutable per-acquisition lease identity. */
  readonly leaseId: string
  /** Absolute workspace path of the worktree. */
  readonly path: string
  /** Absolute root of the repository the worktree belongs to. */
  readonly repoRoot: string
}

/**
 * Testable worktree-pool seam. `acquire` must be idempotent for one
 * `leaseKey`: the executor derives the key from the provision fingerprint, so
 * a retry adopts the same lease instead of cutting a second worktree. Real
 * implementations keep a process-local key→lease map and may adopt a leased
 * worktree found through the git pool's `listWorktrees` after a restart.
 */
export interface GraphOperatorWorktreePool {
  acquire(leaseKey: string): Promise<GraphOperatorWorktreeLease>
  release(lease: GraphOperatorWorktreeLease): Promise<void>
}

export interface AgentGraphOperatorExecutorOptions {
  readonly store: GraphControlStore
  readonly pool: GraphOperatorWorktreePool
  readonly childRunner: GraphOperatorChildRunner
  /** Commits one terminal source event per settled activation (record source). */
  readonly recordSink?: (event: AgentGraphRecordSourceEvent) => Promise<void>
  /** Generates the runtime event id of each emitted record. */
  readonly newId: () => string
  /**
   * Optional cap on concurrently running child starts across all operators
   * (default: unlimited). Per-operator activation remains serialized
   * regardless of this hint.
   */
  readonly concurrencyHint?: number
}
