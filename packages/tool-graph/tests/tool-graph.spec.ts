import { afterEach, describe, expect, it } from 'vitest'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Config, SqliteStorageBackend } from '@deepseek-ai/dsh-storage-sqlite'
import { GraphControlStore } from '@hy-sde-org/dsh-graph-control'
import type { AgentGraphOperatorProvisionRequest } from '@hy-sde-org/dsh-graph-control'
import type {
  AgentGraphExecutor,
  AgentGraphRecord,
  AgentGraphRecordSource,
  AgentGraphRecordSourceEvent,
  AgentGraphRunClaimedIntentInput,
} from '@hy-sde-org/dsh-graph-stream'
import { graphRecordId } from '@hy-sde-org/dsh-graph-stream'
import { Context } from '@deepseek-ai/cordis'
import { brandString } from '@deepseek-ai/dsh-brand'
import { ToolCallId } from '@deepseek-ai/dsh-llm'
import { ToolRuntime } from '@deepseek-ai/dsh-tools'
import { SystemPrompt } from '@deepseek-ai/dsh-system-prompt'
import { SessionProjectionRegistry } from '@deepseek-ai/dsh-session-projection'
import { Session, SESSION_FORMAT_VERSION } from '@deepseek-ai/dsh-session'
import type { SessionId } from '@deepseek-ai/dsh-session'
import { createAgentGraphController } from '../src/controller.ts'
import { registerAgentGraphTools, type ToolCallIdentity } from '../src/tools.ts'
import toolGraphPackage from '../src/index.ts'
import type { AgentGraphController } from '../src/controller.ts'

const GRAPH = 'graph_g1'
const ROOT = 'session-root'

const dirs: string[] = []
afterEach(async () => {
  for (const dir of dirs.splice(0)) await rm(dir, { recursive: true, force: true })
})

async function freshPath(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'dsh-tool-graph-'))
  dirs.push(dir)
  return join(dir, 'graph.db')
}

async function openStore(path: string): Promise<GraphControlStore> {
  const backend = new SqliteStorageBackend(new Config({ path }))
  const unit = await backend.kv.open(GraphControlStore.descriptor)
  return GraphControlStore.open(unit)
}

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
      if (state === 'cancelled') throw new Error('agent graph: admission cancelled')
    }
    const records = this.sink.emit(input.intent, input.claim)
    this.runsByClaim.set(input.claim.claimId, records)
    return records
  }

  async stopSession(sessionId: string) {
    this.stops.push(sessionId)
  }
}

function must<T>(value: T | undefined): T {
  if (value === undefined) throw new Error('expected a defined value')
  return value
}

class InMemoryRecordSource implements AgentGraphRecordSource {
  private readonly events = new Map<string, AgentGraphRecordSourceEvent[]>()
  private seq = 0

  constructor(private readonly graphId: string) { }

  async listCommittedEvents(operatorId: string, sessionId: string) {
    return [...(this.events.get(`${operatorId}\u0000${sessionId}`) ?? [])]
  }

  emit(
    intent: AgentGraphRunClaimedIntentInput['intent'],
    claim: AgentGraphRunClaimedIntentInput['claim'],
  ): AgentGraphRecord[] {
    const key = `${intent.operatorId}\u0000${intent.targetSessionId}`
    this.seq += 1
    const runtimeEventId = `evt-${this.seq}`
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
        this.graphId,
        intent.operatorId,
        intent.targetSessionId,
        claim.targetRunId,
        runtimeEventId,
      ),
      graphId: this.graphId,
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
}

let callSeq = 0

function makeCall(overrides: Partial<ToolCallIdentity> = {}): ToolCallIdentity {
  callSeq += 1
  return {
    sessionId: ROOT,
    turnId: `turn-${String(callSeq)}`,
    toolCallId: `call-${String(callSeq)}`,
    ...overrides,
  }
}

async function makeFixture(options: { drive?: boolean } = {}): Promise<{
  store: GraphControlStore
  controller: AgentGraphController
  view: ReturnType<typeof registerAgentGraphTools>[0]
  update: ReturnType<typeof registerAgentGraphTools>[1]
  yieldTool: ReturnType<typeof registerAgentGraphTools>[2]
  recordSource: InMemoryRecordSource
  executor: FakeExecutor
}> {
  const path = await freshPath()
  const store = await openStore(path)
  const recordSource = new InMemoryRecordSource(GRAPH)
  const executor = new FakeExecutor(store, recordSource)
  const controller = createAgentGraphController({
    store,
    rootSessionId: ROOT,
    newId: () => `id-${String(callSeq)}`,
    options: {
      executor,
      recordSource,
      maxNewActivations: 4,
      newId: () => `id-${String(callSeq)}`,
    },
  })
  // Keep the coordinator's background reconcile drive quiescent so tool
  // assertions are deterministic; the drive is exercised explicitly below.
  if (options.drive !== true) controller.getOrCreate(GRAPH).stop()
  const [view, update, yieldTool] = registerAgentGraphTools({ controller })
  return { store, controller, view, update, yieldTool, recordSource, executor }
}

/* ------------------------------ spec ------------------------------ */

describe('view_agent_graph', () => {
  it('views an empty graph without error, with zero omissions and no cursor', async () => {
    const { view } = await makeFixture()
    const snapshot = await view.execute({ graphId: GRAPH }, makeCall())
    expect(snapshot.graphId).toBe(GRAPH)
    expect(snapshot.revision).toBe(0)
    expect(snapshot.closed).toBe(false)
    expect(snapshot.work).toEqual([])
    expect(snapshot.records).toEqual([])
    expect(snapshot.omitted).toEqual({
      work: 0,
      stoppedTargets: 0,
      records: 0,
      partialRecords: 0,
      readiness: 0,
    })
    expect(snapshot.nextCursor).toBeUndefined()
  })

  it('rejects a non-root caller before touching graph state', async () => {
    const { view } = await makeFixture()
    await expect(
      view.execute({ graphId: GRAPH }, makeCall({ sessionId: 'session-attacker' })),
    ).rejects.toMatchObject({ code: 'not_root_session' })
  })

  it('pages live state with a returned cursor', async () => {
    const { update, view } = await makeFixture()
    for (let batch = 0; batch < 3; batch += 1) {
      const items = Array.from({ length: 32 }, (_, index) => ({
        subagentId: `preset-${String(batch)}-${String(index)}`,
        instruction: `work ${String(batch)}/${String(index)}`,
        inputIds: [],
      }))
      await update.execute({ graphId: GRAPH, addWork: items }, makeCall())
    }
    const first = await view.execute({ graphId: GRAPH }, makeCall())
    expect(first.work).toHaveLength(64)
    expect(first.omitted.work).toBe(32)
    expect(first.nextCursor).toMatch(/^work:/)
    const second = await view.execute(
      { graphId: GRAPH, ...(first.nextCursor !== undefined ? { cursor: first.nextCursor } : {}) },
      makeCall(),
    )
    expect(second.work).toHaveLength(32)
    expect(second.omitted.work).toBe(64)
    expect(second.nextCursor).toBeUndefined()
  })

  it('rejects an unknown cursor', async () => {
    const { view } = await makeFixture()
    await expect(
      view.execute({ graphId: GRAPH, cursor: 'record:nope' }, makeCall()),
    ).rejects.toMatchObject({ code: 'invalid_input' })
  })
})

describe('update_agent_graph', () => {
  it('adds work with a deterministic id and a live projection', async () => {
    const { update, view, executor } = await makeFixture()
    const result = await update.execute(
      {
        graphId: GRAPH,
        addWork: [
          {
            subagentId: 'preset-port',
            instruction: 'Port the P4 tools',
            inputIds: ['r1', 'r2'],
          },
        ],
      },
      makeCall(),
    )
    expect(result.update.created).toBe(true)
    expect(result.update.revision).toBe(1)
    expect(result.graph.work).toHaveLength(1)
    const work = must(result.graph.work[0])
    expect(work.workId).toMatch(/^graph_work_[0-9a-f]{32}$/)
    expect(work.target).toEqual({ kind: 'preset', id: 'preset-port' })
    expect(work.status).toBe('requested')
    expect(work.instruction).toBe('Port the P4 tools')
    expect(work.inputIds).toEqual(['r1', 'r2'])
    const seen = await view.execute({ graphId: GRAPH }, makeCall())
    expect(seen.work).toHaveLength(1)
    expect(must(seen.work[0]).workId).toBe(work.workId)
    expect(executor.runCount).toBe(0)
  })

  it('rejects 33 addWork items', async () => {
    const { update } = await makeFixture()
    const items = Array.from({ length: 33 }, (_, index) => ({
      subagentId: `preset-${String(index)}`,
      instruction: `work ${String(index)}`,
      inputIds: [],
    }))
    await expect(
      update.execute({ graphId: GRAPH, addWork: items }, makeCall()),
    ).rejects.toMatchObject({ code: 'invalid_input' })
  })

  it('rejects 65 input ids in one update', async () => {
    const { update } = await makeFixture()
    await expect(
      update.execute(
        {
          graphId: GRAPH,
          addWork: [{
            subagentId: 'preset-a',
            instruction: 'wide frontier',
            inputIds: Array.from({ length: 65 }, (_, index) => `r-${String(index)}`),
          }],
        },
        makeCall(),
      ),
    ).rejects.toMatchObject({ code: 'invalid_input' })
  })

  it('rejects a 60001-char instruction', async () => {
    const { update } = await makeFixture()
    await expect(
      update.execute(
        {
          graphId: GRAPH,
          addWork: [{ subagentId: 'preset-a', instruction: 'x'.repeat(60_001), inputIds: [] }],
        },
        makeCall(),
      ),
    ).rejects.toMatchObject({ code: 'invalid_input' })
  })

  it('rejects a stop list over the 20-target bound', async () => {
    const { update } = await makeFixture()
    await expect(
      update.execute(
        {
          graphId: GRAPH,
          stop: Array.from({ length: 21 }, (_, index) => ({
            targetId: `w-${String(index)}`,
            reason: 'boundary',
          })),
        },
        makeCall(),
      ),
    ).rejects.toMatchObject({ code: 'invalid_input' })
  })

  it('rejects replaces targeting unknown work', async () => {
    const { update } = await makeFixture()
    await expect(
      update.execute(
        {
          graphId: GRAPH,
          addWork: [{ subagentId: 'preset-a', instruction: 'x', replaces: 'no-such-work', inputIds: [] }],
        },
        makeCall(),
      ),
    ).rejects.toMatchObject({ code: 'invalid_input' })
  })

  it('rejects a work item replacing itself (self supersede)', async () => {
    const { update } = await makeFixture()
    await expect(
      update.execute(
        {
          graphId: GRAPH,
          addWork: [{
            workId: 'w-self',
            subagentId: 'preset-a',
            instruction: 'x',
            replaces: 'w-self',
            inputIds: [],
          }],
        },
        makeCall(),
      ),
    ).rejects.toMatchObject({ code: 'invalid_input' })
  })

  it('accepts an explicit workId (idempotent identity extension)', async () => {
    const { update } = await makeFixture()
    const result = await update.execute(
      {
        graphId: GRAPH,
        addWork: [{
          workId: 'w-explicit',
          subagentId: 'preset-a',
          instruction: 'named',
          inputIds: [],
        }],
      },
      makeCall(),
    )
    expect(must(result.graph.work[0]).workId).toBe('w-explicit')
  })

  it('rejects finish while non-terminal work is pending', async () => {
    const { update } = await makeFixture()
    await update.execute(
      { graphId: GRAPH, addWork: [{ subagentId: 'preset-a', instruction: 'pending', inputIds: [] }] },
      makeCall(),
    )
    await expect(
      update.execute({ graphId: GRAPH, finish: { resultIds: ['r1'], reason: 'done' } }, makeCall()),
    ).rejects.toMatchObject({ code: 'invalid_input' })
  })

  it('cleans provider-fill junk and trimmed instruction through the target discriminator', async () => {
    const { update, view } = await makeFixture()
    const result = await update.execute(
      {
        graphId: GRAPH,
        operation: 'add_work',
        addWork: [{
          targetKind: 'new_preset',
          subagentId: 'preset-port',
          agentId: 'legacy-ignored',
          operatorId: 'operator-ignored',
          instruction: '  Clean the provider-fill payload " ) }  ',
          inputIds: ['r1'],
          '" ) }': 'filler',
        }],
      },
      makeCall(),
    )
    const work = must(result.graph.work[0])
    expect(work.target).toEqual({ kind: 'preset', id: 'preset-port' })
    expect(work.instruction).toBe('Clean the provider-fill payload " ) }')
    const seen = await view.execute({ graphId: GRAPH }, makeCall())
    expect(seen.work).toHaveLength(1)
    expect(must(seen.work[0])).toEqual(work)
  })

  it('dedupes an identical retried update with the same idempotencyKey', async () => {
    const { update, store } = await makeFixture()
    const key = 'retry-key-1'
    const args = {
      graphId: GRAPH,
      addWork: [{ subagentId: 'preset-a', instruction: 'do once', inputIds: [] }],
      idempotencyKey: key,
    }
    const first = await update.execute(args, makeCall())
    const second = await update.execute(args, makeCall())
    expect(first.update.created).toBe(true)
    expect(second.update.created).toBe(false)
    expect(second.update.revision).toBe(first.update.revision)
    expect(second.graph.work).toHaveLength(first.graph.work.length)
    const snapshot = await store.snapshot()
    expect(snapshot.scheduleUpdates).toHaveLength(1)
  })

  it('does not dedupe a different key', async () => {
    const { update, store } = await makeFixture()
    const makeArgs = (key: string) => ({
      graphId: GRAPH,
      addWork: [{ subagentId: 'preset-a', instruction: 'do twice', inputIds: [] }],
      idempotencyKey: key,
    })
    await update.execute(makeArgs('key-1'), makeCall())
    await update.execute(makeArgs('key-2'), makeCall())
    const snapshot = await store.snapshot()
    expect(snapshot.scheduleUpdates).toHaveLength(2)
  })

  it('rejects a non-root caller', async () => {
    const { update } = await makeFixture()
    await expect(
      update.execute({ graphId: GRAPH, addWork: [{ subagentId: 'a', instruction: 'x', inputIds: [] }] }, makeCall({ sessionId: 'other' })),
    ).rejects.toMatchObject({ code: 'not_root_session' })
  })
})

describe('yield_agent_graph', () => {
  it('yields with pending work: delivers a wake and reports pending count', async () => {
    const { update, yieldTool, store } = await makeFixture()
    await update.execute(
      { graphId: GRAPH, addWork: [{ subagentId: 'preset-a', instruction: 'pending', inputIds: [] }] },
      makeCall(),
    )
    const result = await yieldTool.execute({ graphId: GRAPH }, makeCall())
    expect(result.deliveredOnIdle).toBe(true)
    expect(result.wakeId).toMatch(/^graph_wake_/)
    expect(result.pendingWorkCount).toBe(1)
    const snapshot = await store.snapshot()
    expect(snapshot.supervisorWakes).toHaveLength(1)
    expect(snapshot.supervisorWakes[0]).toMatchObject({
      graphId: GRAPH,
      rootSessionId: ROOT,
      status: 'pending',
    })
  })

  it('rejects nothing_to_yield when no work or claims exist', async () => {
    const { yieldTool } = await makeFixture()
    await expect(yieldTool.execute({ graphId: GRAPH }, makeCall())).rejects.toMatchObject({
      code: 'nothing_to_yield',
    })
  })

  it('rejects a non-root caller', async () => {
    const { yieldTool } = await makeFixture()
    await expect(
      yieldTool.execute({ graphId: GRAPH }, makeCall({ sessionId: 'other' })),
    ).rejects.toMatchObject({ code: 'not_root_session' })
  })
})

describe('coordinator drive', () => {
  it('claims and admits scheduled work on an explicit reconcile', async () => {
    const { controller, update, executor, store } = await makeFixture({ drive: true })
    await update.execute(
      { graphId: GRAPH, addWork: [{ subagentId: 'preset-a', instruction: 'run me', inputIds: [] }] },
      makeCall(),
    )
    await controller.getOrCreate(GRAPH).reconcileAndWait()
    expect(executor.runCount).toBe(1)
    const snapshot = await store.snapshot()
    expect(snapshot.intentClaims).toHaveLength(1)
    const claim = must((await store.listAgentGraphIntentClaims(GRAPH))[0])
    expect(claim.admissionStatus).toBe('executing')
  })
})

describe('controller.stop', () => {
  it('stops one work item through the controller', async () => {
    const { controller, update } = await makeFixture()
    const result = await update.execute(
      { graphId: GRAPH, addWork: [{ subagentId: 'preset-a', instruction: 'stop me', inputIds: [] }] },
      makeCall(),
    )
    const workId = must(result.graph.work[0]).workId
    const stopped = await controller.stop(
      GRAPH,
      { targetId: workId, reason: 'superseded by review' },
      { sessionId: ROOT, runId: 'run-stop', turnId: 'turn-stop', toolCallId: 'call-stop', orchestrationMode: 'graph' },
    )
    expect(stopped.projection.work.find(work => work.workId === workId)?.status).toBe('stopped')
  })

  it('rejects a stop targeting unknown work', async () => {
    const { controller } = await makeFixture()
    await expect(
      controller.stop(
        GRAPH,
        { targetId: 'no-such', reason: 'x' },
        { sessionId: ROOT, runId: 'run-stop', turnId: 'turn-stop', toolCallId: 'call-stop', orchestrationMode: 'graph' },
      ),
    ).rejects.toMatchObject({ code: 'invalid_input' })
  })
})

describe('plugin composition', () => {
  it('mounts the tools and runs view through the real tool runtime', async () => {
    const fixture = await makeFixture()
    const ctx = new Context()
    await ctx.plugin(SystemPrompt)
    await ctx.plugin(ToolRuntime)
    await ctx.plugin(SessionProjectionRegistry)
    ctx.provide('agentGraphController', fixture.controller)
    await ctx.plugin(toolGraphPackage, {})
    const sessionId = brandString<SessionId>(ROOT)
    const session = Session.create(sessionId, [], {
      version: SESSION_FORMAT_VERSION,
      id: sessionId,
      createdAt: Date.now(),
      isSeeded: false,
    })
    const agent = { session } as never
    const result = await ctx.tools.execute({
      signal: new AbortController().signal,
      callId: ToolCallId('call-composed'),
      name: 'view_agent_graph',
      arguments: { graphId: GRAPH },
      agent,
    })
    expect(result.isError).toBe(false)
    expect(result.value).toEqual(expect.objectContaining({ graphId: GRAPH, revision: 0 }))
    await ctx.fiber.dispose()
  })

  it('mounts without the host controller and fails every graph tool call loud', async () => {
    // Regression: an agent preset carrying tool-graph must not break session
    // creation just because the optional graph host assembly is absent (for
    // example after a server restart that predates the graph rows, or when
    // rootSessionId is stale). The tools degrade: each call errors clearly.
    const ctx = new Context()
    await ctx.plugin(SystemPrompt)
    await ctx.plugin(ToolRuntime)
    await ctx.plugin(SessionProjectionRegistry)
    await ctx.plugin(toolGraphPackage, {})
    const sessionId = brandString<SessionId>(ROOT)
    const session = Session.create(sessionId, [], {
      version: SESSION_FORMAT_VERSION,
      id: sessionId,
      createdAt: Date.now(),
      isSeeded: false,
    })
    const agent = { session } as never
    const result = await ctx.tools.execute({
      signal: new AbortController().signal,
      callId: ToolCallId('call-no-host'),
      name: 'view_agent_graph',
      arguments: { graphId: GRAPH },
      agent,
    })
    expect(result.isError).toBe(true)
    expect(result.error?.message).toContain('[agent-graph-unavailable]')
    await ctx.fiber.dispose()
  })

  it('rejects a non-root session through the real tool runtime', async () => {
    const fixture = await makeFixture()
    const ctx = new Context()
    await ctx.plugin(SystemPrompt)
    await ctx.plugin(ToolRuntime)
    await ctx.plugin(SessionProjectionRegistry)
    ctx.provide('agentGraphController', fixture.controller)
    await ctx.plugin(toolGraphPackage, {})
    const sessionId = brandString<SessionId>('session-other')
    const session = Session.create(sessionId, [], {
      version: SESSION_FORMAT_VERSION,
      id: sessionId,
      createdAt: Date.now(),
      isSeeded: false,
    })
    const agent = { session } as never
    const result = await ctx.tools.execute({
      signal: new AbortController().signal,
      callId: ToolCallId('call-other'),
      name: 'view_agent_graph',
      arguments: { graphId: GRAPH },
      agent,
    })
    expect(result.isError).toBe(true)
    expect(result.error?.message).toContain('[not_root_session]')
    await ctx.fiber.dispose()
  })
})
