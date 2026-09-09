import { describe, expect, it } from 'vitest'
import type {
  AgentGraphRecordSourceEvent,
  AgentGraphRecordSource,
} from '../src/projection.ts'
import {
  graphRecordId,
  readCommittedAgentGraphProjection,
} from '../src/projection.ts'
import { compareAgentGraphIdentity } from '../src/identity.ts'
import {
  buildAgentGraphTraceSnapshot,
  graphEdgeId,
  graphRouteId,
  validateAgentGraphTraceTopology,
} from '../src/trace.ts'
import { projectAgentGraphSchedule } from '../src/schedule-projection.ts'
import type { AgentGraphScheduleUpdate } from '../src/types.ts'
import { buildAgentGraphReadinessSnapshot } from '../src/readiness.ts'
/* ------------------------------ identity ------------------------------ */

describe('graph identity order', () => {
  it('orders by UTF-16 code units, not locale', () => {
    expect(compareAgentGraphIdentity('a', 'b')).toBeLessThan(0)
    // Code-unit order, not locale order: 'ä' (U+00E4) sorts AFTER 'z' (U+007A).
    expect(compareAgentGraphIdentity('ä', 'z')).toBeGreaterThan(0)
    expect(compareAgentGraphIdentity('same', 'same')).toBe(0)
  })
})

/* --------------------------- schedule projection ---------------------- */

function update(
  overrides: Partial<AgentGraphScheduleUpdate> = {},
): AgentGraphScheduleUpdate {
  return {
    schemaVersion: 1,
    updateId: 'graph_update_x',
    updateFingerprint: 'fp',
    graphId: 'g1',
    source: { sessionId: 's', runId: 'r', turnId: 't', toolCallId: 'c' },
    addWork: [],
    stop: [],
    revision: 1,
    committedAt: 1,
    ...overrides,
  }
}

function work(
  workId: string,
  overrides: Partial<AgentGraphScheduleUpdate['addWork'][number]> = {},
): AgentGraphScheduleUpdate['addWork'][number] {
  return {
    workId,
    target: { kind: 'operator', id: 'op_a' },
    instruction: `task ${workId}`,
    inputIds: [],
    ...overrides,
  }
}

describe('projectAgentGraphSchedule', () => {
  it('derives requested/stopped/superseded and closed', () => {
    const projection = projectAgentGraphSchedule('g1', [
      update({
        updateId: 'u1',
        revision: 1,
        addWork: [work('w1'), work('w2')],
      }),
      update({
        updateId: 'u2',
        revision: 2,
        stop: [{ targetId: 'w1', reason: 'obsolete' }],
      }),
      update({
        updateId: 'u3',
        revision: 3,
        addWork: [work('w3', { replaces: 'w2' })],
      }),
      update({
        updateId: 'u4',
        revision: 4,
        finish: { resultIds: ['r1'], reason: 'done' },
      }),
    ])
    expect(projection.closed).toBe(true)
    expect(projection.revision).toBe(4)
    expect(projection.updateCount).toBe(4)
    expect(projection.work.map(item => [item.workId, item.status])).toEqual([
      ['w1', 'stopped'],
      ['w2', 'superseded'],
      ['w3', 'requested'],
    ])
    expect(projection.stoppedTargets).toHaveLength(1)
    expect(projection.finish?.reason).toBe('done')
  })

  it('rejects non-contiguous revisions and updates after finish', () => {
    expect(() =>
      projectAgentGraphSchedule('g1', [
        update({ updateId: 'u1', revision: 2 }),
      ]),
    ).toThrow(/non-contiguous/)
    expect(() =>
      projectAgentGraphSchedule('g1', [
        update({
          updateId: 'u1',
          revision: 1,
          finish: { resultIds: [], reason: 'x' },
        }),
        update({ updateId: 'u2', revision: 2 }),
      ]),
    ).toThrow(/after finish/)
  })
})

/* -------------------------------- trace ------------------------------- */

describe('trace topology', () => {
  const OPS = ['op1', 'op2', 'op3']
  const EDGE = () => ({
    edgeId: graphEdgeId('g', 'w1', 'op1', 'op2'),
    fromOperatorId: 'op1',
    toOperatorId: 'op2',
  })

  it('rejects unknown operators, self-loops, duplicate endpoints, and cycles', () => {
    expect(() =>
      validateAgentGraphTraceTopology('g', OPS, [
        { edgeId: 'e1', fromOperatorId: 'op1', toOperatorId: 'phantom' },
      ]),
    ).toThrow(/unknown operator/)
    expect(() =>
      validateAgentGraphTraceTopology('g', OPS, [
        { edgeId: 'e1', fromOperatorId: 'op1', toOperatorId: 'op1' },
      ]),
    ).toThrow(/self-loop/)
    expect(() =>
      validateAgentGraphTraceTopology('g', OPS, [
        { edgeId: 'e1', fromOperatorId: 'op1', toOperatorId: 'op2' },
        { edgeId: 'e2', fromOperatorId: 'op1', toOperatorId: 'op2' },
      ]),
    ).toThrow(/multiple edges/)
    expect(() =>
      validateAgentGraphTraceTopology('g', OPS, [
        { edgeId: 'e1', fromOperatorId: 'op1', toOperatorId: 'op2' },
        { edgeId: 'e2', fromOperatorId: 'op2', toOperatorId: 'op1' },
      ]),
    ).toThrow(/cycle/)
  })
  it('derives one route per (edge × emitted record) with deterministic ids', () => {
    const edge = EDGE()
    const emitted = new Map<
      string,
      readonly import('../src/types.ts').AgentGraphRecord[]
    >([
      [
        'op1',
        [
          {
            recordId: 'graph_record_a',
            graphId: 'g',
            operatorId: 'op1',
            source: { sessionId: 's', runId: 'r', seq: 1 },
            summary: 'x',
            facets: ['message'] as const,
            emittedAt: 1,
          },
        ],
      ],
    ])
    const { topology, routes } = buildAgentGraphTraceSnapshot(
      'g',
      OPS,
      [edge],
      emitted,
    )
    expect(topology.edges).toHaveLength(1)
    expect(routes).toHaveLength(1)
    expect(routes[0]?.routeId).toBe(graphRouteId('g', edge, 'graph_record_a'))
    expect(routes[0]?.sourceActivationId).toBe('s:r')
    expect(routes[0]?.targetOperatorId).toBe('op2')
  })
})
/* ----------------------------- record fold ---------------------------- */

describe('record projection', () => {
  it('ignores partial events, stops after a terminal, and sorts deterministically', async () => {
    const eventsBySession: Record<string, AgentGraphRecordSourceEvent[]> = {
      s1: [
        {
          runtimeEventId: 'evt-1',
          seq: 1,
          runId: 'r1',
          summary: 'first',
          terminal: false,
          emittedAt: 1,
        },
        {
          runtimeEventId: 'evt-p',
          seq: 2,
          runId: 'r1',
          summary: 'partial',
          terminal: false,
          partial: true,
          emittedAt: 2,
        },
        {
          runtimeEventId: 'evt-2',
          seq: 3,
          runId: 'r1',
          summary: 'final',
          terminal: true,
          emittedAt: 3,
        },
        {
          runtimeEventId: 'evt-3',
          seq: 4,
          runId: 'r1',
          summary: 'after',
          terminal: false,
          emittedAt: 4,
        },
      ],
    }
    const sourceImpl: AgentGraphRecordSource = {
      listCommittedEvents: (_operatorId: string, sessionId: string) =>
        Promise.resolve(eventsBySession[sessionId] ?? []),
    }
    const state = await readCommittedAgentGraphProjection(
      'g',
      [{ operatorId: 'op1', sessionId: 's1' }],
      sourceImpl,
    )
    expect(state.records.map(record => record.summary)).toEqual([
      'first',
      'final',
    ])
    expect(state.omittedPartialCount).toBe(1)
    expect(state.operators[0]?.terminal).toBe(true)
    expect(state.records[0]?.recordId).toBe(
      graphRecordId('g', 'op1', 's1', 'r1', 'evt-1'),
    )
  })
})

/* ------------------------------- readiness ---------------------------- */

describe('readiness snapshot (map policy)', () => {
  it('derives one intent per received route with deterministic ids', () => {
    const edge = {
      edgeId: graphEdgeId('g', 'w1', 'op_up', 'op_down'),
      fromOperatorId: 'op_up',
      toOperatorId: 'op_down',
    }
    const records = [
      {
        recordId: 'graph_record_a',
        graphId: 'g',
        operatorId: 'op_up',
        source: { sessionId: 's-up', runId: 'r1', seq: 1 },
        summary: 'x',
        facets: ['message', 'terminal'] as const,
        emittedAt: 1,
      },
    ]
    const state = buildAgentGraphReadinessSnapshot({
      graphId: 'g',
      operators: [
        { operatorId: 'op_up', sessionId: 's-up' },
        { operatorId: 'op_down', sessionId: 's-down' },
      ],
      edges: [edge],
      records,
      policies: [{ readinessId: 'r1', operatorId: 'op_down', kind: 'map' }],
    })
    expect(state.routesCount).toBe(1)
    expect(state.intents).toHaveLength(1)
    expect(state.intents[0]?.operatorId).toBe('op_down')
    expect(state.intents[0]?.targetSessionId).toBe('s-down')
    expect(state.intents[0]?.triggerRecordIds).toEqual(['graph_record_a'])
    expect(state.operatorStates[0]?.status).toBe('runnable')
    expect(state.intents[0]?.intentId).toMatch(/^graph_intent_[0-9a-f]{32}$/)
  })
  it('waits with upstream ids when no routes', () => {
    const edge = {
      edgeId: graphEdgeId('g', 'w1', 'op_up', 'op_down'),
      fromOperatorId: 'op_up',
      toOperatorId: 'op_down',
    }
    const state = buildAgentGraphReadinessSnapshot({
      graphId: 'g',
      operators: [
        { operatorId: 'op_up', sessionId: 's-up' },
        { operatorId: 'op_down', sessionId: 's-down' },
      ],
      edges: [edge],
      records: [],
      policies: [{ readinessId: 'r1', operatorId: 'op_down', kind: 'map' }],
    })
    expect(state.intents).toHaveLength(0)
    expect(state.operatorStates[0]?.status).toBe('waiting')
    expect(state.operatorStates[0]?.waitingFor).toEqual([
      { kind: 'input_route', upstreamOperatorIds: ['op_up'] },
    ])
  })
})
