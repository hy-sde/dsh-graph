/**
 * Pure types of the graph-projection domain: the ONE home of the `graph`
 * projection-key declaration and the `graph/change` session event, free of
 * this package's host-side value imports (zod, the projection definition).
 * Host consumers import `./types` for the wire contract; the session log
 * stores the event this package augments.
 *
 * @module @hy-sde-org/dsh-graph-projection/types
 */

import type { SessionSeqCursor } from '@deepseek-ai/dsh-session/types'

export {}

/** Execution-lifetime status of one graph work item as the host reports it. */
export type SessionGraphWorkStatus =
  | 'requested'
  | 'claimed'
  | 'executing'
  | 'stopped'
  | 'finished'
  | 'failed'

/** One bounded work item on the graph rail. */
export interface SessionGraphWorkEntry {
  readonly workId: string
  readonly status: SessionGraphWorkStatus
  /** Bounded instruction preview (≤300 chars, ellipsis when clipped) — the host truncates before publishing. */
  readonly instruction: string
  /** The operator binding once the work is provisioned; absent while merely requested. */
  readonly operatorId?: string | undefined
  readonly inputCount: number
}

/**
 * The client view of one session's agent graph: the complete published
 * snapshot, plain JSON. `status` is the closed/open summary derived by the
 * host; `closed` mirrors its terminal state for consumers that branch on it.
 */
export interface SessionGraphProjection {
  readonly schemaVersion: 1
  readonly graphId: string
  readonly status: 'active' | 'closed'
  /** Host-assigned publish revision, strictly increasing per graph. */
  readonly revision: number
  readonly closed: boolean
  readonly work: readonly SessionGraphWorkEntry[]
  /** Bounded-view overflows the host capped away from the rail. */
  readonly omitted: {
    readonly work: number
    readonly records: number
    readonly inputs: number
  }
  /** Whether a supervisor wake is pending delivery to the root session. */
  readonly pendingWake: boolean
  readonly updatedAt: number
}

/**
 * Fold state: the last accepted `graph/change` payload plus the fold
 * watermark. `snapshot` is `null` before the first event, and the registry
 * serves `null` through the client view — the session hosts no graph yet.
 * `asOfSeq` advances on every `graph/change` event the unit observes
 * (including a foreign-graph publish, which keeps the standing snapshot);
 * `revision` is the revision of the last ACCEPTED payload.
 */
export interface SessionGraphProjectionState {
  /** The standing graph snapshot; `null` before the first matching event. */
  readonly snapshot: SessionGraphProjection | null
  /** Seq of the last `graph/change` event folded; -1 before any. */
  readonly asOfSeq: SessionSeqCursor
  /** Revision of the last folded graph/change payload; 0 before any. */
  readonly revision: number
}

declare module '@deepseek-ai/dsh-session-projection/types' {
  interface SessionProjectionStateMap {
    /** Agent-graph fold state for one session (last graph snapshot plus watermark). */
    graph: SessionGraphProjectionState
  }
  interface SessionProjectionMap {
    /**
     * The session's current agent-graph snapshot, or `null` before the first
     * `graph/change` event (no graph hosted yet). The host publishes whole
     * post-change values; consumers replace, never merge.
     */
    graph: SessionGraphProjection | null
  }
}

declare module '@deepseek-ai/dsh-session/types' {
  interface SessionEventMap {
    /**
     * Complete post-publish agent-graph snapshot. Log-only (not a
     * {@link SurfaceEventType}); the host appends one per graph state change
     * so the projection stays a normal event fold with no store coupling.
     * @mode emit
     * @param graphId - the graph the snapshot belongs to.
     * @param snapshot - the whole current client view at the publish.
     * @param revision - the host raise counter for this publish; strictly
     *   increasing per graph.
     */
    'graph/change': {
      graphId: string
      snapshot: SessionGraphProjection
      revision: number
    }
  }
}
