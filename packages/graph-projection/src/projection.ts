/**
 * The `graph` projection unit: a pure fold of the host's `graph/change`
 * publishes into the session's standing agent-graph snapshot.
 *
 * The host (P7) owns graph state and publishes the complete post-change
 * `SessionGraphProjection` whenever the graph moves — the projection unit is
 * a normal event fold with no store coupling. The fold replaces its state
 * only for the graph it already hosts: one DSH session owns one graph, so a
 * publish naming a different `graphId` keeps the standing snapshot (the
 * watermark still advances). A publish whose revision does not advance past
 * the last folded one is a stale re-publish: `apply` returns the same state
 * reference, and the registry's `Object.is` gates then hold the change feed
 * quiet. Because the served view is the event payload's snapshot reference,
 * a foreign-graph publish also stays quiet through the identity gate.
 *
 * @module @hy-sde-org/dsh-graph-projection/projection
 */

import { z } from 'zod'
import type { ZodType } from 'zod'
import { SessionSeq } from '@deepseek-ai/dsh-session'
import type { SessionEventMap, SessionSeqCursor } from '@deepseek-ai/dsh-session/types'
import type { ProjectionDefinition } from '@deepseek-ai/dsh-session-projection'
import type {
  SessionGraphProjection,
  SessionGraphProjectionState,
  SessionGraphWorkEntry,
} from './types.ts'

/** Rail-card instruction budget: the host truncates with an ellipsis before publishing. */
export const GRAPH_PROJECTION_MAX_INSTRUCTION_CHARS = 300

const workEntrySchema: ZodType<SessionGraphWorkEntry> = z.object({
  workId: z.string().min(1),
  status: z.enum([
    'requested',
    'claimed',
    'executing',
    'stopped',
    'finished',
    'failed',
  ]),
  instruction: z.string().max(GRAPH_PROJECTION_MAX_INSTRUCTION_CHARS),
  // `?: string | undefined` pairs with zod's Optional output under
  // `exactOptionalPropertyTypes`; readers see the same `string | undefined`.
  operatorId: z.string().min(1).optional(),
  inputCount: z.number().int().nonnegative(),
}).strict()

const sessionGraphProjectionSchema: ZodType<SessionGraphProjection> = z.object({
  schemaVersion: z.literal(1),
  graphId: z.string().min(1),
  status: z.enum(['active', 'closed']),
  revision: z.number().int().positive(),
  closed: z.boolean(),
  work: z.array(workEntrySchema),
  omitted: z.object({
    work: z.number().int().nonnegative(),
    records: z.number().int().nonnegative(),
    inputs: z.number().int().nonnegative(),
  }).strict(),
  pendingWake: z.boolean(),
  updatedAt: z.number().nonnegative(),
}).strict()

/** One session-event-seq cursor: -1 for the empty log, branded seqs afterwards. */
const cursorSchema = z.number().int().min(-1).transform(
  (value): SessionSeqCursor => value === -1 ? -1 : SessionSeq(value),
)

const graphProjectionStateSchema: ZodType<SessionGraphProjectionState> = z.object({
  snapshot: sessionGraphProjectionSchema.nullable(),
  asOfSeq: cursorSchema,
  // 0 before the first accepted publish (revisions start at 1).
  revision: z.number().int().nonnegative(),
}).strict()

const EMPTY_STATE: SessionGraphProjectionState = { snapshot: null, asOfSeq: -1, revision: 0 }

/**
 * Build the `graph/change` payload for one publish. The host appends it with
 * `session.append('graph/change', graphSnapshotToEvent(graphId, snapshot, revision))`
 * after committing the graph state, so the projection unit folds exactly what
 * the rest of the log already carries.
 * @param graphId - the graph the snapshot belongs to.
 * @param snapshot - the whole current client view at the publish.
 * @param revision - the host raise counter for this publish; strictly
 *   increasing per graph and equal to `snapshot.revision`.
 * @returns the `graph/change` event payload.
 */
export function graphSnapshotToEvent(
  graphId: string,
  snapshot: SessionGraphProjection,
  revision: number,
): SessionEventMap['graph/change'] {
  return { graphId, snapshot, revision }
}

/** The `graph` unit registered on `ctx.sessionProjections` (exported for the unit spec). */
export const graphProjectionDefinition = {
  key: 'graph',
  /** Persisted-cache invalidation version: bump whenever the serialized state fields or the fold semantics change. */
  stateVersion: 1,
  stateSchema: graphProjectionStateSchema,
  init: () => EMPTY_STATE,
  apply: (state, event) => {
    switch (event.type) {
      case 'graph/change': {
        const payload = event.data
        if (state.snapshot !== null) {
          if (payload.graphId !== state.snapshot.graphId) {
            // A foreign graph never displaces the standing one: the snapshot
            // keeps its reference (the identity gate then keeps the change
            // feed quiet) while the watermark records the observed event.
            return { ...state, asOfSeq: event.seq }
          }
          // A stale re-publish (same revision) or a regressive one holds the
          // standing state; the host bumps the revision per publish.
          if (payload.revision <= state.revision) return state
        }
        return {
          snapshot: payload.snapshot,
          asOfSeq: event.seq,
          revision: payload.revision,
        }
      }
      default:
        return state
    }
  },
  wire: {
    viewSchema: sessionGraphProjectionSchema.nullable(),
    view: state => state.snapshot,
  },
} satisfies ProjectionDefinition<'graph', SessionGraphProjectionState>
