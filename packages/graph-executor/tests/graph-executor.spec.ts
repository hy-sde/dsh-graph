import { afterEach, describe, expect, it } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Config, SqliteStorageBackend } from '@deepseek-ai/dsh-storage-sqlite'
import { AgentGraphOperatorExecutor, createGraphOperatorExecutor, provisionKey } from '../src/index.ts'
import type {
  AgentGraphChildRun,
  GraphOperatorChildRunner,
  GraphOperatorChildStartInput,
  GraphOperatorWorktreeLease,
  GraphOperatorWorktreePool,
} from '../src/index.ts'
import { GraphControlStore } from '@hy-sde-org/dsh-graph-control'
import type {
  AgentGraphIntentClaim,
  AgentGraphOperatorProvisionRequest,
} from '@hy-sde-org/dsh-graph-control'
import type {
  AgentGraphRecordSourceEvent,
  AgentGraphRunnableIntent,
  AgentGraphRunClaimedIntentInput,
} from '@hy-sde-org/dsh-graph-stream'

/** Executor adapter over faked runner/pool seams and a real sqlite-backed store. */

const GRAPH = 'graph_g1'

class FakeWorktreePool implements GraphOperatorWorktreePool {
  readonly leases = new Map<string, GraphOperatorWorktreeLease>()
  readonly released: GraphOperatorWorktreeLease[] = []
  acquireCount = 0
  failNextAcquire = false
  private leaseSeq = 0

  async acquire(leaseKey: string): Promise<GraphOperatorWorktreeLease> {
    if (this.failNextAcquire) {
      this.failNextAcquire = false
      throw new Error('worktree pool unavailable')
    }
    this.acquireCount += 1
    const existing = this.leases.get(leaseKey)
    if (existing !== undefined) return existing
    this.leaseSeq += 1
    const lease: GraphOperatorWorktreeLease = {
      leaseId: `lease-${this.leaseSeq}`,
      path: `/wt/${leaseKey}`,
      repoRoot: '/repo/root',
    }
    this.leases.set(leaseKey, lease)
    return lease
  }

  async release(lease: GraphOperatorWorktreeLease): Promise<void> {
    this.released.push(lease)
  }
}

class FakeChildRunner implements GraphOperatorChildRunner {
  readonly starts: GraphOperatorChildStartInput[] = []
  readonly stops: { sessionId: string; opts?: { reason?: string } }[] = []
  defaultRun: AgentGraphChildRun = { outcome: 'fulfilled', summary: 'operator done' }
  private readonly pending: ((run: AgentGraphChildRun) => void)[] = []

  async start(input: GraphOperatorChildStartInput): Promise<AgentGraphChildRun> {
    this.starts.push(input)
    return new Promise<AgentGraphChildRun>((resolve) => {
      this.pending.push(resolve)
    })
  }

  resolveNext(run: AgentGraphChildRun = this.defaultRun): void {
    const resolve = this.pending.shift()
    if (resolve === undefined) throw new Error('no pending child start')
    resolve(run)
  }

  async stop(sessionId: string, opts?: { reason?: string }): Promise<void> {
    this.stops.push({ sessionId, ...(opts !== undefined ? { opts } : {}) })
  }
}

let seq = 0
function newId(): string {
  seq += 1
  return `id-${seq}`
}

function provisionRequest(overrides: Partial<AgentGraphOperatorProvisionRequest> = {}): AgentGraphOperatorProvisionRequest {
  return {
    provisionId: 'graph_provision_1',
    graphId: GRAPH,
    workId: 'work-1',
    operatorId: 'graph_operator_1',
    targetSessionId: 'child-1',
    initialTurnId: 'turn-1',
    initialRunId: 'run-1',
    provisionFingerprint: 'fp-1',
    edges: [],
    expectedScheduleRevision: 0,
    ...overrides,
  }
}

function intent(overrides: Partial<AgentGraphRunnableIntent> = {}): AgentGraphRunnableIntent {
  return {
    schemaVersion: 1,
    intentId: 'graph_intent_1',
    graphId: GRAPH,
    readinessContextFingerprint: 'ctx-1',
    policyFingerprint: 'pol-1',
    readinessId: 'work-1',
    operatorId: 'graph_operator_1',
    targetSessionId: 'child-1',
    inputIds: [],
    selectedResultInputs: [],
    policyKind: 'supervisor',
    triggerRouteIds: [],
    triggerRecordIds: [],
    ...overrides,
  }
}

function claim(overrides: Partial<AgentGraphIntentClaim> = {}): AgentGraphIntentClaim {
  return {
    schemaVersion: 1,
    claimId: 'graph_claim_1',
    graphId: GRAPH,
    intentId: 'graph_intent_1',
    intentFingerprint: 'ifp-1',
    readinessContextFingerprint: 'ctx-1',
    targetOperatorId: 'graph_operator_1',
    targetSessionId: 'child-1',
    targetTurnId: 'turn-1',
    targetRunId: 'run-1',
    claimedAt: 1,
    ...overrides,
  }
}

function runInput(
  overrides: Partial<AgentGraphRunClaimedIntentInput> = {},
): AgentGraphRunClaimedIntentInput {
  return {
    intent: intent(),
    claim: claim(),
    prompt: 'prompt-1',
    ...overrides,
  }
}

const tmpDirs: string[] = []
function tmpPath(): string {
  const dir = mkdtempSync(join(tmpdir(), 'graph-executor-'))
  tmpDirs.push(dir)
  return join(dir, 'test.db')
}

function must<T>(value: T | undefined): T {
  if (value === undefined) throw new Error('expected a defined value')
  return value
}

function tick(): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, 0))
}

let current: { backend: SqliteStorageBackend; store: GraphControlStore } | undefined

async function openStore(path: string): Promise<{
  backend: SqliteStorageBackend
  store: GraphControlStore
  unit: unknown
}> {
  const backend = new SqliteStorageBackend(new Config({ path }))
  const unit = await backend.kv.open(GraphControlStore.descriptor)
  const store = await GraphControlStore.open(unit)
  return { backend, store, unit }
}

async function setup(path: string) {
  const env = await openStore(path)
  current = env
  const pool = new FakeWorktreePool()
  const runner = new FakeChildRunner()
  const events: AgentGraphRecordSourceEvent[] = []
  const executor = new AgentGraphOperatorExecutor({
    store: env.store,
    pool,
    childRunner: runner,
    recordSink: async (event) => {
      events.push(event)
    },
    newId,
  })
  return { ...env, pool, runner, events, executor }
}

afterEach(async () => {
  if (current !== undefined) {
    await current.store.close().catch(() => {})
    await current.backend.close().catch(() => {})
    current = undefined
  }
  for (const dir of tmpDirs.splice(0)) rmSync(dir, { recursive: true, force: true })
  seq = 0
})

/* ------------------------------- provision ---------------------------- */

describe('provisionOperator', () => {
  it('acquires one deterministic lease per fingerprint and persists the binding', async () => {
    const env = await setup(tmpPath())
    const request = provisionRequest()
    const result = await env.executor.provisionOperator(request)
    expect(result?.created).toBe(true)
    expect(must(result).provision.provisionId).toBe(request.provisionId)
    expect(env.pool.acquireCount).toBe(1)
    const leaseKey = provisionKey(request)
    expect([...env.pool.leases.keys()]).toEqual([leaseKey])

    const binding = await env.store.readOperatorBinding(request.provisionId)
    expect(binding?.leaseId).toBe('lease-1')
    expect(binding?.path).toBe(`/wt/${leaseKey}`)
    expect(binding?.repoRoot).toBe('/repo/root')
    expect(binding?.graphId).toBe(GRAPH)
    expect(binding?.workId).toBe('work-1')
    expect(await env.store.listOperatorProvisions(GRAPH)).toHaveLength(1)
    expect(await env.store.listOperatorBindings(GRAPH)).toHaveLength(1)

    // A retry of the same request adopts the same lease (pool idempotency).
    const retry = await env.executor.provisionOperator(request)
    expect(retry?.created).toBe(false)
    expect(env.pool.acquireCount).toBe(2)
    expect(env.pool.leases.size).toBe(1)
    expect((await env.store.readOperatorBinding(request.provisionId))?.leaseId).toBe('lease-1')
  })

  it('returns undefined on lease acquisition failure and writes nothing', async () => {
    const env = await setup(tmpPath())
    env.pool.failNextAcquire = true
    const result = await env.executor.provisionOperator(provisionRequest())
    expect(result).toBeUndefined()
    expect(await env.store.listOperatorBindings(GRAPH)).toHaveLength(0)
    expect(await env.store.listOperatorProvisions(GRAPH)).toHaveLength(0)
  })

  it('fails loud when the bound lease differs (CAS reject across pool state)', async () => {
    const env = await setup(tmpPath())
    const request = provisionRequest()
    await env.executor.provisionOperator(request)
    // Simulate pool state loss: a fresh lease is acquired for the same key by mistake.
    env.pool.leases.clear()
    await expect(env.executor.provisionOperator(request)).rejects.toMatchObject({
      code: 'binding-conflict',
    })
  })
})

/* --------------------------------- run -------------------------------- */

describe('runClaimedAgentGraphIntent', () => {
  it('runs the child in the bound worktree and emits one terminal record', async () => {
    const env = await setup(tmpPath())
    await env.executor.provisionOperator(provisionRequest())
    const leaseKey = provisionKey(provisionRequest())
    const input = runInput()
    const run = env.executor.runClaimedAgentGraphIntent(input)
    await tick()
    const started = must(env.runner.starts[0])
    expect(started.sessionId).toBe('child-1')
    expect(started.instructions).toBe('prompt-1')
    expect(started.workspace).toBe(`/wt/${leaseKey}`)
    expect(started.runId).toBe('run-1')
    expect(started.labels).toEqual({
      graphId: GRAPH,
      operatorId: 'graph_operator_1',
      workId: 'work-1',
    })
    env.runner.resolveNext({ outcome: 'fulfilled', summary: 'result text' })
    const records = await run
    expect(records).toHaveLength(1)
    expect(records[0]?.summary).toBe('result text')
    expect(records[0]?.operatorId).toBe('graph_operator_1')
    expect(env.events).toHaveLength(1)
    const event = must(env.events[0])
    expect(event.terminal).toBe(true)
    expect(event.partial).toBe(false)
    expect(event.facets).toEqual(['message', 'terminal'])
    expect(event.runId).toBe('run-1')
    expect(event.summary).toBe('result text')
    expect(event.runtimeEventId).toBe('id-1')
    expect(event.emittedAt).toBeGreaterThan(0)
  })

  it('resolves the bound worktree for operator-targeted work through its provision', async () => {
    const env = await setup(tmpPath())
    await env.executor.provisionOperator(provisionRequest())
    // work-2 re-runs the provisioned operator; only work-1 has a binding.
    const input = runInput({
      intent: intent({ intentId: 'i2', readinessId: 'work-2' }),
      claim: claim({ claimId: 'c2', targetRunId: 'run-2' }),
    })
    const run = env.executor.runClaimedAgentGraphIntent(input)
    await tick()
    expect(env.runner.starts).toHaveLength(1)
    expect(env.runner.starts[0]?.workspace).toBe(`/wt/${provisionKey(provisionRequest())}`)
    env.runner.resolveNext()
    const records = await run
    expect(records[0]?.operatorId).toBe('graph_operator_1')
    expect(records[0]?.source.runId).toBe('run-2')
  })

  it('serializes activations of the same operator (no overlap)', async () => {    const env = await setup(tmpPath())
    await env.executor.provisionOperator(provisionRequest())
    const first = env.executor.runClaimedAgentGraphIntent(
      runInput({ intent: intent({ intentId: 'i1' }), claim: claim({ claimId: 'c1', targetRunId: 'run-a' }) }),
    )
    await tick()
    expect(env.runner.starts).toHaveLength(1)
    const second = env.executor.runClaimedAgentGraphIntent(
      runInput({ intent: intent({ intentId: 'i2' }), claim: claim({ claimId: 'c2', targetRunId: 'run-b' }) }),
    )
    await tick()
    expect(env.runner.starts).toHaveLength(1)
    env.runner.resolveNext()
    await tick()
    expect(env.runner.starts).toHaveLength(2)
    env.runner.resolveNext()
    expect((await first)[0]?.source.runId).toBe('run-a')
    expect((await second)[0]?.source.runId).toBe('run-b')
    expect(env.events).toHaveLength(2)
  })

  it('truncates the terminal summary to 16 KiB before emitting', async () => {
    const env = await setup(tmpPath())
    await env.executor.provisionOperator(provisionRequest())
    const long = 'x'.repeat(20_000)
    const run = env.executor.runClaimedAgentGraphIntent(runInput())
    await tick()
    env.runner.resolveNext({ outcome: 'fulfilled', summary: long })
    const records = await run
    expect(Buffer.byteLength(must(env.events[0]).summary, 'utf8')).toBeLessThanOrEqual(16 * 1024)
    expect(records[0]?.summary.endsWith('…')).toBe(true)
  })

  it('emits a terminal failed event with the error message when the child has no summary', async () => {
    const env = await setup(tmpPath())
    await env.executor.provisionOperator(provisionRequest())
    const run = env.executor.runClaimedAgentGraphIntent(runInput())
    await tick()
    env.runner.resolveNext({ outcome: 'failed', error: new Error('boom') })
    const records = await run
    expect(records).toHaveLength(1)
    expect(records[0]?.summary).toBe('[operator failed] boom')
  })

  it('never starts the child when admission returns cancelled', async () => {
    const env = await setup(tmpPath())
    await env.executor.provisionOperator(provisionRequest())
    const records = await env.executor.runClaimedAgentGraphIntent(
      runInput({ admitExecution: async () => 'cancelled' }),
    )
    expect(records).toEqual([])
    expect(env.runner.starts).toHaveLength(0)
    expect(env.events).toHaveLength(0)
  })
})

/* --------------------------------- stop ------------------------------- */

describe('stopSession', () => {
  it('delegates to the child runner with the supervisor source mapped to a reason', async () => {
    const env = await setup(tmpPath())
    await env.executor.stopSession('child-1', { source: 'graph_supervisor' })
    await env.executor.stopSession('child-2')
    expect(env.runner.stops).toEqual([
      { sessionId: 'child-1', opts: { reason: 'graph_supervisor' } },
      { sessionId: 'child-2' },
    ])
  })
})

/* ------------------------------ durability ---------------------------- */

describe('reopen durability', () => {
  it('rebinds the same lease after a store reopen and keeps the binding', async () => {
    const path = tmpPath()
    const pool = new FakeWorktreePool()
    const runner = new FakeChildRunner()
    const first = await openStore(path)
    const executor = createGraphOperatorExecutor({
      store: first.store,
      pool,
      childRunner: runner,
      newId,
    })
    const request = provisionRequest()
    await executor.provisionOperator(request)
    const original = must(await first.store.readOperatorBinding(request.provisionId))
    await first.store.close()
    await first.backend.close()

    const reopened = await openStore(path)
    const executor2 = createGraphOperatorExecutor({
      store: reopened.store,
      pool,
      childRunner: new FakeChildRunner(),
      newId,
    })
    const result = await executor2.provisionOperator(request)
    expect(result?.created).toBe(false)
    const binding = must(await reopened.store.readOperatorBinding(request.provisionId))
    expect(binding.leaseId).toBe(original.leaseId)
    expect(binding.boundAt).toBe(original.boundAt)
    expect(await reopened.store.listOperatorBindings(GRAPH)).toHaveLength(1)
    await reopened.store.close()
    await reopened.backend.close()
  })
})
