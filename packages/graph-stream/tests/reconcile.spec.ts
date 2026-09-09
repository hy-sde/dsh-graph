import { afterEach, describe, expect, it } from 'vitest'
import {
  Config,
  SqliteStorageBackend,
} from '@deepseek-ai/dsh-storage-sqlite'
import { GraphControlStore } from '@hy-sde-org/dsh-graph-control'
import type { AgentGraphOperatorProvisionRequest } from '@hy-sde-org/dsh-graph-control'
import type {
  AgentGraphRecordSource,
  AgentGraphRecordSourceEvent,
} from '../src/projection.ts'
import {
  graphRecordId,
  readCommittedAgentGraphProjection,
} from '../src/projection.ts'
import {
  hydrateAgentGraphInputHandoffs,
  renderAgentGraphScheduledWorkPrompt,
} from '../src/handoff.ts'
import {
  reconcileAgentGraphSchedule,
  type AgentGraphReconcileSeams,
} from '../src/reconcile.ts'
import { AgentGraphCoordinator } from '../src/coordinator.ts'
import type {
  AgentGraphExecutor,
  AgentGraphRecord,
  AgentGraphReconciliationTopology,
  AgentGraphRunClaimedIntentInput,
  AgentGraphSupervisorObservation,
} from '../src/types.ts'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
const GRAPH = 'graph_g1'
/* ------------------------------ fixtures ------------------------------ */

class FakeExecutor implements AgentGraphExecutor {
  runCount = 0
  stops: string[] = []
  private readonly runsByClaim = new Map<string, AgentGraphRecord[]>()
  constructor(
    private readonly store: GraphControlStore,
    private readonly sink: InMemoryRecordSource,
  ) { }

  async provisionOperator(request: AgentGraphOperatorProvisionRequest) {
    return this.store.provisionOperator(request)
  }

  async runClaimedAgentGraphIntent(
    input: AgentGraphRunClaimedIntentInput,
  ): Promise<AgentGraphRecord[]> {
    const existing = this.runsByClaim.get(input.claim.claimId)
    if (existing !== undefined) return existing
    this.runCount += 1
    if (input.admitExecution !== undefined) {
      const state = await input.admitExecution()
      if (state === 'cancelled')
        throw new Error('agent graph: admission cancelled')
    }
    const records = this.sink.emit(input.intent, input.claim)
    this.runsByClaim.set(input.claim.claimId, records)
    return records
  }

  async stopSession(sessionId: string) {
    this.stops.push(sessionId)
  }
}

class InMemoryRecordSource implements AgentGraphRecordSource {
  private readonly events = new Map<string, AgentGraphRecordSourceEvent[]>()

  async listCommittedEvents(operatorId: string, sessionId: string) {
    return [...(this.events.get(`${operatorId}\u0000${sessionId}`) ?? [])]
  }

  emit(
    intent: AgentGraphRunClaimedIntentInput['intent'],
    claim: AgentGraphRunClaimedIntentInput['claim'],
  ): AgentGraphRecord[] {
    const key = `${intent.operatorId}\u0000${intent.targetSessionId}`
    const runtimeEventId = `evt-${claim.claimId}`
    const events = this.events.get(key) ?? []
    events.push({
      runtimeEventId,
      seq: events.length + 1,
      runId: claim.targetRunId,
      summary: `result-of-${intent.readinessId}`,
      terminal: true,
      facets: ['message', 'terminal'],
      emittedAt: events.length + 1,
    })
    this.events.set(key, events)
    const record: AgentGraphRecord = {
      recordId: graphRecordId(
        GRAPH,
        intent.operatorId,
        intent.targetSessionId,
        claim.targetRunId,
        runtimeEventId,
      ),
      graphId: GRAPH,
      operatorId: intent.operatorId,
      source: {
        sessionId: intent.targetSessionId,
        runId: claim.targetRunId,
        runtimeEventId,
        seq: events.length,
      },
      summary: `result-of-${intent.readinessId}`,
      facets: ['message', 'terminal'],
      emittedAt: events.length,
    }
    return [record]
  }

  lastRecordFor(
    operatorId: string,
    sessionId: string,
  ): AgentGraphRecord | undefined {
    const events = this.events.get(`${operatorId}\u0000${sessionId}`) ?? []
    const last = events[events.length - 1]
    if (last === undefined) return undefined
    return {
      recordId: graphRecordId(
        GRAPH,
        operatorId,
        sessionId,
        last.runId,
        last.runtimeEventId,
      ),
      graphId: GRAPH,
      operatorId,
      source: {
        sessionId,
        runId: last.runId,
        runtimeEventId: last.runtimeEventId,
        seq: last.seq,
      },
      summary: last.summary,
      facets: last.facets ?? [],
      emittedAt: last.emittedAt,
    }
  }
}

let seq = 0
function newId(): string {
  seq += 1
  return `id-${seq}`
}

function updateRequest(overrides: Partial<ReturnType<typeof baseUpdate>> = {}) {
  seq += 1
  return {
    ...baseUpdate(),
    updateId: `graph_update_${seq}`,
    ...overrides,
  }
}

function baseUpdate() {
  return {
    schemaVersion: 1 as const,
    updateId: 'x',
    updateFingerprint: `fp-${seq}`,
    graphId: GRAPH,
    source: {
      sessionId: 'root-1',
      runId: `run-${seq}`,
      turnId: `turn-${seq}`,
      toolCallId: `call-${seq}`,
    },
    addWork: [] as {
      workId: string
      target: { kind: 'agent'; id: string } | { kind: 'operator'; id: string }
      instruction: string
      inputIds: string[]
      replaces?: string
    }[],
    stop: [] as { targetId: string; reason: string }[],
  }
}

function work(
  workId: string,
  overrides: Partial<{
    target: { kind: 'agent'; id: string } | { kind: 'operator'; id: string }
    instruction: string
    inputIds: string[]
    replaces?: string
  }> = {},
) {
  return {
    workId,
    target: overrides.target ?? { kind: 'agent', id: 'agent-a' },
    instruction: overrides.instruction ?? `do ${workId}`,
    inputIds: overrides.inputIds ?? [],
    ...(overrides.replaces !== undefined
      ? { replaces: overrides.replaces }
      : {}),
  }
}

async function setup(path: string) {
  const backend = new SqliteStorageBackend(new Config({ path }))
  const unit = await backend.kv.open(GraphControlStore.descriptor)
  const store = await GraphControlStore.open(unit)
  const sink = new InMemoryRecordSource()
  const executor = new FakeExecutor(store, sink)
  const observe = async (topology: AgentGraphReconciliationTopology): Promise<AgentGraphSupervisorObservation> => {
    const state = await readCommittedAgentGraphProjection(
      topology.graphId,
      topology.operators,
      sink,
    )
    return {
      graphId: topology.graphId,
      records: state.records,
      operators: state.operators,
    }
  }
  const seams: AgentGraphReconcileSeams = {
    store,
    executor,
    recordSource: sink,
    observeGraph: observe,
    newId,
    maxNewActivations: 4,
    renderPrompt: input =>
      `${input.work.instruction}\n\n${input.inputHandoffs.length}`,
  }
  return { backend, unit, store, sink, executor, observe, seams }
}

const tmpDirs: string[] = []
function tmpPath(): string {
  const dir = mkdtempSync(join(tmpdir(), 'graph-stream-'))
  tmpDirs.push(dir)
  return join(dir, 'test.db')
}

function must<T>(value: T | undefined): T {
  if (value === undefined) throw new Error('expected a defined value')
  return value
}

afterEach(() => {
  for (const dir of tmpDirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true })
  }
  seq = 0
})
/* ------------------------------ reconcile ----------------------------- */

describe('reconcileAgentGraphSchedule', () => {
  it('provisions, claims, and runs a single agent work end-to-end', async () => {
    const env = await setup(tmpPath())
    await env.store.commitScheduleUpdate(
      updateRequest({ addWork: [work('w1')] }),
    )
    const result = await reconcileAgentGraphSchedule({
      ...env.seams,
      graphId: GRAPH,
    })
    expect(result.status).toBe('reconciled')
    expect(result.dispatches).toHaveLength(1)
    expect(result.newActivationCount).toBe(1)
    expect(result.failures).toHaveLength(0)
    const provisions = await env.store.listOperatorProvisions(GRAPH)
    expect(provisions).toHaveLength(1)
    const provision = must(provisions[0])
    expect(provision.workId).toBe('w1')
    expect(env.executor.runCount).toBe(1)
    const record = env.sink.lastRecordFor(
      provision.operatorId,
      provision.targetSessionId,
    )
    expect(record?.summary).toBe('result-of-w1')
    await env.backend.close()
  })
  it('defers input_not_committed until the input record exists, then runs', async () => {
    const env = await setup(tmpPath())
    await env.store.commitScheduleUpdate(
      updateRequest({ addWork: [work('w1')] }),
    )
    await env.store.commitScheduleUpdate(
      updateRequest({
        addWork: [
          work('w2', {
            instruction: 'do w2',
            target: { kind: 'agent', id: 'agent-b' },
            inputIds: ['graph_record_pending'],
          }),
        ],
      }),
    )
    const result = await reconcileAgentGraphSchedule({
      ...env.seams,
      graphId: GRAPH,
    })
    // w1 dispatched; w2 cannot run yet.
    expect(result.status).toBe('waiting')
    expect(result.deferred).toEqual(
      expect.arrayContaining([{ workId: 'w2', reason: 'input_not_committed' }]),
    )
    expect(result.dispatches.map(item => item.workId)).toEqual(['w1'])
    await env.backend.close()
  })
  it('stops unclaimed work as cancelled_before_runtime', async () => {
    const env = await setup(tmpPath())
    await env.store.commitScheduleUpdate(
      updateRequest({ addWork: [work('w1')] }),
    )
    await env.store.commitScheduleUpdate(
      updateRequest({ stop: [{ targetId: 'w1', reason: 'obsolete' }] }),
    )
    const result = await reconcileAgentGraphSchedule({
      ...env.seams,
      graphId: GRAPH,
    })
    expect(result.status).toBe('reconciled')
    expect(result.stops).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          targetId: 'w1',
          status: 'cancelled_before_runtime',
        }),
      ]),
    )
    expect(result.dispatches).toHaveLength(0)
    expect(env.executor.runCount).toBe(0)
    await env.backend.close()
  })
  it('superseded work is never dispatched; its target gets a stop', async () => {
    const env = await setup(tmpPath())
    await env.store.commitScheduleUpdate(
      updateRequest({ addWork: [work('w1')] }),
    )
    await env.store.commitScheduleUpdate(
      updateRequest({ addWork: [work('w2', { replaces: 'w1' })] }),
    )
    const result = await reconcileAgentGraphSchedule({
      ...env.seams,
      graphId: GRAPH,
    })
    expect(result.status).toBe('reconciled')
    expect(result.dispatches.map(item => item.workId)).toEqual(['w2'])
    expect(result.stops).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          targetId: 'w1',
          status: 'cancelled_before_runtime',
        }),
      ]),
    )
    const provisions = await env.store.listOperatorProvisions(GRAPH)
    expect(provisions.map(provision => provision.workId)).toEqual(['w2'])
    await env.backend.close()
  })
})

/* ------------------------------ coordinator --------------------------- */

describe('AgentGraphCoordinator', () => {
  it('commits, runs once, observes existing claims, and recovers on reopen', async () => {
    const path = tmpPath()
    let env = await setup(path)
    const coordinator = new AgentGraphCoordinator({
      graphId: GRAPH,
      store: env.store,
      executor: env.executor,
      recordSource: env.sink,
      newId,
      maxNewActivations: 4,
      observeGraph: env.observe,
    })
    const commit = await coordinator.scheduleUpdate(
      updateRequest({ addWork: [work('w1')] }),
    )
    expect(commit.update.revision).toBe(1)
    expect(commit.created).toBe(true)
    const first = await coordinator.reconcileAndWait()
    expect(first.status).toBe('reconciled')
    expect(first.dispatches).toHaveLength(1)
    expect(env.executor.runCount).toBe(1)

    const second = await coordinator.reconcileAndWait()
    expect(second.observedExistingActivationCount).toBe(1)
    expect(env.executor.runCount).toBe(1) // same claim → same run (idempotent executor)

    await env.backend.close()

    // Reopen the same medium with fresh objects: recover must not re-create the claim.
    env = await setup(path)
    const recovered = new AgentGraphCoordinator({
      graphId: GRAPH,
      store: env.store,
      executor: env.executor,
      recordSource: env.sink,
      newId,
      maxNewActivations: 4,
      observeGraph: env.observe,
    })
    const after = await recovered.recover()
    expect(after.status).toBe('reconciled')
    expect(after.observedExistingActivationCount).toBe(1)
    expect(env.executor.runCount).toBe(1) // fresh executor observed the existing run (no new claim created)
    const claims = await env.store.listAgentGraphIntentClaims(GRAPH)
    expect(claims.filter(claim => claim.graphId === GRAPH)).toHaveLength(1)
    await env.backend.close()
  })
})

/* -------------------------------- handoff ----------------------------- */

describe('handoff hydrate + render', () => {
  it('truncates at 16 KiB per record, escapes handoff JSON, includes protocol', async () => {
    const long = 'x'.repeat(20 * 1024)
    const record: AgentGraphRecord = {
      recordId: 'graph_record_a',
      graphId: GRAPH,
      operatorId: 'op1',
      source: { sessionId: 's1', runId: 'r1', runtimeEventId: 'evt-1', seq: 1 },
      summary: long,
      facets: ['message'],
      emittedAt: 1,
    }
    const handoffs = await hydrateAgentGraphInputHandoffs({
      records: [record],
      resolver: { resolveConclusionText: async () => long },
    })
    expect(handoffs).toHaveLength(1)
    expect(handoffs[0]?.conclusion?.textTruncated).toBe(true)
    expect(
      Buffer.byteLength(handoffs[0]?.conclusion?.text ?? '', 'utf8'),
    ).toBeLessThanOrEqual(16 * 1024)

    const angled: AgentGraphRecord = {
      recordId: 'graph_record_b',
      graphId: GRAPH,
      operatorId: 'op1',
      source: { sessionId: 's1', runId: 'r1', runtimeEventId: 'evt-2', seq: 2 },
      summary: '<b>found</b>',
      facets: ['message'],
      emittedAt: 2,
    }
    const angledHandoffs = await hydrateAgentGraphInputHandoffs({
      records: [angled],
      resolver: {
        resolveConclusionText: async () => '<b>found</b> & <i>ok</i>',
      },
    })
    const prompt = renderAgentGraphScheduledWorkPrompt({
      instruction: 'do <x>',
      inputHandoffs: angledHandoffs,
    })
    expect(prompt).toContain('<agent_graph_handoff_protocol>')
    expect(prompt).toContain('do <x>')
    expect(prompt).toContain('\\u003c')
  })
})
