/**
 * The `graph` projection unit: mounting the plugin beside the projection
 * registry serves the session's standing agent-graph snapshot folded from
 * committed `graph/change` publishes; compositions without the registry are
 * unaffected; unmounting the plugin removes the key (HMR safety). The fold
 * replaces state only for the standing graph, gates stale revisions and
 * foreign-graph publishes on identity, and the helper builds the publish
 * payload the host appends.
 */

import { describe, expect, expectTypeOf, it } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import SessionStore, { SessionId, SessionLogOffset, SessionSeq } from '@deepseek-ai/dsh-session'
import type { Session, SessionEvent } from '@deepseek-ai/dsh-session'
import type { SessionSeqCursor } from '@deepseek-ai/dsh-session/types'
import SessionProjectionRegistry from '@deepseek-ai/dsh-session-projection'
import type {
  SessionProjectionMap,
  SessionProjectionStateMap,
} from '@deepseek-ai/dsh-session-projection/types'
import * as GraphProjectionPlugin from '../src/index.ts'
import {
  GRAPH_PROJECTION_MAX_INSTRUCTION_CHARS,
  graphProjectionDefinition,
  graphSnapshotToEvent,
} from '../src/projection.ts'
import type {
  SessionGraphProjection,
  SessionGraphProjectionState,
  SessionGraphWorkEntry,
} from '../src/types.ts'

/** Branded checkpoint cursor: -1 for the empty log, `SessionSeq` otherwise (mirror of the definition's cursor schema). */
function cursor(value: number): SessionSeqCursor {
  return value === -1 ? -1 : SessionSeq(value)
}

async function harness(withGraphPlugin: boolean): Promise<{ ctx: Context; session: Session }> {
  const ctx = new Context()
  await ctx.plugin(SessionStore)
  await ctx.plugin(SessionProjectionRegistry)
  if (withGraphPlugin) await ctx.plugin(GraphProjectionPlugin)
  return { ctx, session: ctx.sessions.create(SessionId('graphed')) }
}

/** Build one snapshot and append its `graph/change` publish; returns event and snapshot. */
function publish(
  session: Session,
  graphId: string,
  revision: number,
  work: readonly SessionGraphWorkEntry[] = [],
): { event: SessionEvent<'graph/change'>; snapshot: SessionGraphProjection } {
  const snapshot: SessionGraphProjection = {
    schemaVersion: 1,
    graphId,
    status: 'active',
    revision,
    closed: false,
    work,
    omitted: { work: 0, records: 0, inputs: 0 },
    pendingWake: false,
    updatedAt: 1_000_000 + revision,
  }
  const event = session.append('graph/change', graphSnapshotToEvent(graphId, snapshot, revision))
  return { event, snapshot }
}

function graphOf(ctx: Context, session: Session): SessionGraphProjection | null {
  return ctx.sessionProjections.snapshot(session).values.graph ?? null
}

describe('graph projection unit', () => {
  it('declares the graph key in both projection tables with the view as payload', () => {
    expectTypeOf<SessionProjectionMap['graph']>().toEqualTypeOf<SessionGraphProjection | null>()
    expectTypeOf<SessionProjectionStateMap['graph']>().toEqualTypeOf<SessionGraphProjectionState>()
    expectTypeOf<SessionEvent<'graph/change'>['data']['snapshot']>().toEqualTypeOf<SessionGraphProjection>()
  })

  it('has no graph key without the plugin and serves null before the first publish', async () => {
    const { ctx, session } = await harness(false)
    expect('graph' in ctx.sessionProjections.snapshot(session).values).toBe(false)
    const mounted = await harness(true)
    expect(graphOf(mounted.ctx, mounted.session)).toBeNull()
    expect(mounted.ctx.sessionProjections.checkpoint(mounted.session).graph)
      .toEqual({ ver: 1, seq: -1, val: { snapshot: null, asOfSeq: -1, revision: 0 } })
  })

  it('folds committed graph/change publishes and serves the payload snapshot by identity', async () => {
    const { ctx, session } = await harness(true)
    const first = publish(session, 'g-main', 1, [{
      workId: 'w1',
      status: 'requested',
      instruction: 'produce the report',
      inputCount: 2,
    }])
    const second = publish(session, 'g-main', 2)
    expect(graphOf(ctx, session)).toEqual(second.snapshot)
    expect(ctx.sessionProjections.snapshot(session).asOfSeq).toBe(second.event.seq)
    expect(ctx.sessionProjections.stateOf(session, 'graph'))
      .toEqual({ snapshot: second.snapshot, asOfSeq: second.event.seq, revision: 2 })
    expect(first.snapshot.graphId).toBe('g-main')
  })

  it('gates a stale same-revision re-publish (no change, no feed push)', async () => {
    const { ctx, session } = await harness(true)
    const first = publish(session, 'g-main', 1)
    const pushes: SessionEvent<'graph/change'>[] = []
    ctx.sessionProjections.onChanged((changed, key, _value, seq) => {
      if (changed === session && key === 'graph') {
        const event = session.eventAt(seq)
        if (event?.type === 'graph/change') pushes.push(event)
      }
    })
    const before = ctx.sessionProjections.stateOf(session, 'graph')
    publish(session, 'g-main', 1)
    // Stale re-publish: the cell keeps its exact state object (the registry's Object.is gate).
    expect(ctx.sessionProjections.stateOf(session, 'graph')).toBe(before)
    expect(ctx.sessionProjections.stateOf(session, 'graph')?.asOfSeq).toBe(first.event.seq)
    expect(pushes).toEqual([])
    const next = publish(session, 'g-main', 2)
    expect(graphOf(ctx, session)).toEqual(next.snapshot)
    expect(pushes).toEqual([next.event])
  })

  it('keeps the standing snapshot when a different graphId publishes (watermark advances)', async () => {
    const { ctx, session } = await harness(true)
    const pushes: number[] = []
    ctx.sessionProjections.onChanged((changed, key, _value, seq) => {
      if (changed === session && key === 'graph') pushes.push(seq)
    })
    const standing = publish(session, 'g-main', 3)
    expect(pushes).toEqual([standing.event.seq])
    const foreign = publish(session, 'g-other', 1)
    expect(ctx.sessionProjections.stateOf(session, 'graph')?.asOfSeq).toBe(foreign.event.seq)
    // The kept snapshot reference crosses the view identity gate: no push.
    expect(graphOf(ctx, session)).toEqual(standing.snapshot)
    expect(pushes).toEqual([standing.event.seq])
    // The standing graph still accepts its own later publishes.
    const resumed = publish(session, 'g-main', 4)
    expect(graphOf(ctx, session)).toEqual(resumed.snapshot)
    expect(pushes).toEqual([standing.event.seq, resumed.event.seq])
  })

  it('shares one registration across duplicate keys and rejects a version change', async () => {
    const { ctx } = await harness(true)
    expect(() => ctx.sessionProjections.register(graphProjectionDefinition)).not.toThrow()
    expect(() => ctx.sessionProjections.register({
      ...graphProjectionDefinition,
      stateVersion: 9,
    })).toThrow(/already registered at stateVersion 1; refusing to share it with stateVersion 9/)
  })

  it('removes the key when the mounting fiber unloads (HMR safety)', async () => {
    const ctx = new Context()
    await ctx.plugin(SessionStore)
    await ctx.plugin(SessionProjectionRegistry)
    const session = ctx.sessions.create(SessionId('graphed-hmr'))
    const fiber = await ctx.plugin(GraphProjectionPlugin)
    publish(session, 'g-main', 1)
    expect('graph' in ctx.sessionProjections.snapshot(session).values).toBe(true)
    await fiber.dispose()
    expect('graph' in ctx.sessionProjections.snapshot(session).values).toBe(false)
    // Fresh registration folds the log again instead of serving a stale cell.
    await ctx.plugin(GraphProjectionPlugin)
    expect(graphOf(ctx, session)?.revision).toBe(1)
  })

  it('rejects a persisted state whose snapshot revision violates the version schema', async () => {
    const { ctx, session } = await harness(true)
    const standing = publish(session, 'g-main', 1).snapshot
    const bad = {
      graph: {
        ver: 1,
        seq: cursor(0),
        val: {
          snapshot: { ...standing, revision: 0 },
          asOfSeq: 0,
          revision: 0,
        },
      },
    }
    expect(ctx.sessionProjections.viewCheckpoint(bad)).toEqual({})
    expect(() => ctx.sessionProjections.restore(
      bad,
      [],
      SessionLogOffset(1),
      session.header,
      session.inheritedEventCount,
    )).toThrow()
  })

  it('rejects persisted work entries over the instruction bound', async () => {
    const { ctx } = await harness(true)
    const bad = {
      graph: {
        ver: 1,
        seq: cursor(-1),
        val: {
          snapshot: null,
          asOfSeq: -1,
          revision: 0,
        },
      },
    }
    const tooLong = {
      ...bad,
      graph: {
        ...bad.graph,
        val: {
          snapshot: {
            ...graphSnapshotToEvent('g-main', {
              schemaVersion: 1,
              graphId: 'g-main',
              status: 'active',
              revision: 1,
              closed: false,
              work: [{
                workId: 'w1',
                status: 'claimed',
                instruction: 'x'.repeat(GRAPH_PROJECTION_MAX_INSTRUCTION_CHARS + 1),
                inputCount: 0,
              }],
              omitted: { work: 0, records: 0, inputs: 0 },
              pendingWake: false,
              updatedAt: 1,
            }, 1).snapshot,
            asOfSeq: -1,
            revision: 0,
          },
        },
      },
    }
    expect(ctx.sessionProjections.viewCheckpoint(tooLong)).toEqual({})
  })

  it('builds the cell lazily with events already in the log when mounted late', async () => {
    const { ctx, session } = await harness(false)
    publish(session, 'g-main', 1)
    publish(session, 'g-main', 2)
    await ctx.plugin(GraphProjectionPlugin)
    expect(graphOf(ctx, session)?.revision).toBe(2)
    expect(ctx.sessionProjections.stateOf(session, 'graph')?.asOfSeq).toBe(session.seq - 1)
  })

  it('graphSnapshotToEvent returns the exact graph/change payload', () => {
    const snapshot: SessionGraphProjection = {
      schemaVersion: 1,
      graphId: 'g-main',
      status: 'closed',
      revision: 5,
      closed: true,
      work: [],
      omitted: { work: 0, records: 0, inputs: 0 },
      pendingWake: true,
      updatedAt: 42,
    }
    expect(graphSnapshotToEvent('g-main', snapshot, 5)).toEqual({ graphId: 'g-main', snapshot, revision: 5 })
  })
})
