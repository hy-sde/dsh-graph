/**
 * Assembler and plugin wiring tests of the Agent Graph host assembly: the
 * real SqliteStorageBackend + GraphControlStore, fake subagents/worktrees/
 * compaction/session-events/idle, and one plugin smoke test over a stub
 * cordis context.
 * @module
 */

import { describe, expect, it } from 'vitest'
import { mkdtempSync, rmSync, realpathSync } from 'node:fs'
import { spawnSync } from 'node:child_process'
import { readFile, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Context } from '@deepseek-ai/cordis'
import LocalSubprocessRuntime from '@deepseek-ai/dsh-subprocess-local'
import {
  GitService,
  acquireWorktree,
  listWorktrees,
} from '@hy-sde-org/dsh-git'
import type { Agent } from '@deepseek-ai/dsh-agent'
import {
  AGENT_GRAPH_INTENT_CLAIM_SCHEMA_VERSION,
  AGENT_GRAPH_SCHEDULE_SCHEMA_VERSION,
  type AgentGraphIntentClaimRequest,
  type AgentGraphOperatorProvisionRequest,
  type AgentGraphScheduleUpdateRequest,
  type AgentGraphScheduledWork,
} from '@hy-sde-org/dsh-graph-control'
import type { AgentGraphRunClaimedIntentInput } from '@hy-sde-org/dsh-graph-stream'
import { SessionId } from '@deepseek-ai/dsh-session'
import type { Session } from '@deepseek-ai/dsh-session'
import type { ContentBlock } from '@deepseek-ai/dsh-llm'
import type {
  SubagentResult,
  SubagentRun,
  SubagentStartRequest,
  SubagentStopReason,
} from '@deepseek-ai/dsh-subagent'
import { Config, SqliteStorageBackend } from '@deepseek-ai/dsh-storage-sqlite'
import {
  apply,
  createGraphHostServices,
  GraphHostContextOverflowError,
  GraphHostWorktreePool,
  SERVICE_AGENT_GRAPH_CONTROLLER,
  SERVICE_GRAPH_HOST,
} from '../src/index.ts'
import type {
  GraphHostCompaction,
  GraphHostIdle,
  GraphHostSessionEvents,
  GraphHostStorage,
  GraphHostSubagents,
  GraphHostWorktreeEntry,
  GraphHostWorktrees,
} from '../src/types.ts'
import type { SessionEventMap } from '@deepseek-ai/dsh-session/types'

const GRAPH = 'graph_g1'
const ROOT = 'root-1'

/* ------------------------------ utilities ----------------------------- */

function tmpPath(): string {
  return join(mkdtempSync(join(tmpdir(), 'graph-host-')), 'db.sqlite')
}

function cleanup(path: string): void {
  rmSync(path, { recursive: true, force: true })
}

async function waitUntil(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 5_000,
): Promise<void> {
  const start = Date.now()
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) {
      throw new Error('waitUntil: condition not reached')
    }
    await new Promise(resolve => setTimeout(resolve, 10))
  }
}

/* ------------------------------- fakes -------------------------------- */

function must<T>(value: T | undefined): T {
  if (value === undefined) throw new Error('expected a defined value')
  return value
}

class FakeRun implements SubagentRun {
  private static seq = 0
  readonly id = SessionId(`run-${FakeRun.seq++}`)
  readonly localAgent = undefined
  disposed = false
  private readonly resolvers = Promise.withResolvers<SubagentResult>()
  readonly result = this.resolvers.promise

  settle(stopReason: SubagentStopReason, output: ContentBlock[] = []): void {
    this.resolvers.resolve({ output, stopReason })
  }

  async dispose(): Promise<void> {
    this.disposed = true
    this.resolvers.resolve({ output: [], stopReason: 'aborted' })
  }
}

class FakeSubagents implements GraphHostSubagents {
  readonly provider = 'fake'
  readonly starts: { name: string; request: SubagentStartRequest }[] = []
  readonly runs: FakeRun[] = []

  async start(name: string, request: SubagentStartRequest): Promise<SubagentRun> {
    this.starts.push({ name, request })
    const run = new FakeRun()
    this.runs.push(run)
    return run
  }
}

class FakeWorktrees implements GraphHostWorktrees {
  readonly repoRoot = '/repo'
  readonly acquireCalls: { holder: string; branch?: string; signal?: AbortSignal }[] = []
  entries: GraphHostWorktreeEntry[] = []

  async acquire(options: { holder: string; branch?: string; signal?: AbortSignal }): Promise<{
    leaseId: string
    path: string
    repoRoot: string
  }> {
    this.acquireCalls.push(options)
    return { leaseId: options.holder, path: `/worktrees/${options.holder}`, repoRoot: this.repoRoot }
  }

  async list(): Promise<readonly GraphHostWorktreeEntry[]> {
    return [...this.entries]
  }
}

class FakeCompaction implements GraphHostCompaction {
  readonly requests: string[] = []

  async request(sessionId: string): Promise<void> {
    this.requests.push(sessionId)
  }
}

class FakeSessionEvents implements GraphHostSessionEvents {
  readonly events: { sessionId: string; data: SessionEventMap['graph/change'] }[] = []

  async appendGraphChange(sessionId: string, data: SessionEventMap['graph/change']): Promise<boolean> {
    this.events.push({ sessionId, data })
    return true
  }
}

class FakeIdle implements GraphHostIdle {
  private readonly callbacks = new Map<string, Set<(sessionId: string) => void>>()

  observe(rootSessionId: string, onIdle: (sessionId: string) => void): () => void {
    const callbacks = this.callbacks.get(rootSessionId) ?? new Set()
    callbacks.add(onIdle)
    this.callbacks.set(rootSessionId, callbacks)
    return () => {
      callbacks.delete(onIdle)
    }
  }

  fire(sessionId: string): void {
    for (const callback of [...(this.callbacks.get(sessionId) ?? [])]) callback(sessionId)
  }
}

interface Harness {
  path: string
  backend: SqliteStorageBackend
  subagents: FakeSubagents
  worktrees: FakeWorktrees
  compaction: FakeCompaction
  sessionEvents: FakeSessionEvents
  idle: FakeIdle
  services: Awaited<ReturnType<typeof createGraphHostServices>>
}

async function makeHarness(options: { attach?: boolean } = {}): Promise<Harness> {
  const path = tmpPath()
  const backend = new SqliteStorageBackend(new Config({ path }))
  const subagents = new FakeSubagents()
  const worktrees = new FakeWorktrees()
  const compaction = new FakeCompaction()
  const sessionEvents = new FakeSessionEvents()
  const idle = new FakeIdle()
  const storage: GraphHostStorage = { open: descriptor => backend.kv.open(descriptor) }
  const parentAgent = { id: SessionId(ROOT) } as Agent
  const services = await createGraphHostServices({
    rootSessionId: ROOT,
    storage,
    subagents,
    worktrees,
    compaction,
    sessionEvents,
    idle,
    resolveParentAgent: () => parentAgent,
    newId: sequenceId(),
    clock: { now: () => 1_000 },
  })
  if (options.attach === true) await services.attachGraph(GRAPH)
  return { path, backend, subagents, worktrees, compaction, sessionEvents, idle, services }
}

function sequenceId(): () => string {
  let seq = 0
  return () => {
    seq += 1
    return `id-${seq}`
  }
}

function scheduleRequest(input: {
  addWork?: AgentGraphScheduledWork[]
  stop?: { targetId: string; reason: string }[]
  finish?: { resultIds: string[]; reason: string }
} = {}): AgentGraphScheduleUpdateRequest {
  return {
    schemaVersion: AGENT_GRAPH_SCHEDULE_SCHEMA_VERSION,
    updateId: `update_${++scheduleRequest.seq}`,
    updateFingerprint: `fp_${scheduleRequest.seq}`,
    graphId: GRAPH,
    source: {
      sessionId: ROOT,
      runId: `run-src-${scheduleRequest.seq}`,
      turnId: `turn-src-${scheduleRequest.seq}`,
      toolCallId: `call-src-${scheduleRequest.seq}`,
    },
    addWork: input.addWork ?? [],
    stop: input.stop ?? [],
    ...(input.finish !== undefined ? { finish: input.finish } : {}),
  }
}
scheduleRequest.seq = 0

function work(
  workId: string,
  overrides: Partial<{
    target: { kind: 'agent' | 'preset' | 'operator'; id: string }
    instruction: string
    inputIds: string[]
  }> = {},
): AgentGraphScheduledWork {
  return {
    workId,
    target: overrides.target ?? { kind: 'preset', id: 'preset-p1' },
    instruction: overrides.instruction ?? `do ${workId}`,
    inputIds: overrides.inputIds ?? [],
  }
}

/* ------------------------------ projection ---------------------------- */

describe('buildSessionGraphProjection (via snapshotFor)', () => {
  it('bounds work, records, instruction, and reports omitted counts', async () => {
    const harness = await makeHarness({ attach: true })
    const longInstruction = 'x'.repeat(500)
    const manyWork: AgentGraphScheduledWork[] = []
    // No input ids: uncommitted inputs would defer every work (input_not_committed)
    // before provisioning, so nothing would start. This case bounds work+records.
    for (let index = 0; index < 140; index += 1) {
      manyWork.push(work(`w${index}`, { instruction: longInstruction }))
    }
    await harness.services.controller.schedule(GRAPH, scheduleRequest({ addWork: manyWork }))
    // The drive awaits each child run to settle, so one drive starts one child
    // (up to maxNewActivations 4); with the fake harness this means >= 1 per
    // drive. The bounding assertions below stand independent of how many start.
    await waitUntil(() => harness.subagents.starts.length >= 1)

    const snapshot = await harness.services.snapshotFor(GRAPH)
    expect(snapshot.schemaVersion).toBe(1)
    expect(snapshot.status).toBe('active')
    expect(snapshot.omitted.work).toBeGreaterThan(0)
    expect(snapshot.work.length).toBeLessThanOrEqual(128)
    expect(snapshot.omitted.records).toBe(0)

    await harness.services.dispose()
    await harness.backend.close()
    cleanup(harness.path)
  })
})

/* --------------------------- worktree pool ---------------------------- */

describe('GraphHostWorktreePool', () => {
  it('adopts a matching existing slot before minting, then caches locally', async () => {
    const worktrees = new FakeWorktrees()
    worktrees.entries = [
      { name: '0', path: '/adopted', branch: 'graph_operator_lease_abc', leased: true, exists: true },
    ]
    const pool = new GraphHostWorktreePool(worktrees)
    const adopted = await pool.acquire('graph_operator_lease_abc')
    expect(adopted.leaseId).toBe('graph_operator_lease_abc')
    expect(adopted.path).toBe('/adopted')
    expect(worktrees.acquireCalls).toHaveLength(0)

    const minted = await pool.acquire('graph_operator_lease_def')
    expect(minted.leaseId).toBe('graph_operator_lease_def')
    const firstCall = must(worktrees.acquireCalls[0])
    expect(firstCall.holder).toBe('graph_operator_lease_def')
    expect(firstCall.branch).toBe('graph_operator_lease_def')

    const again = await pool.acquire('graph_operator_lease_def')
    expect(again.leaseId).toBe('graph_operator_lease_def')
    expect(worktrees.acquireCalls).toHaveLength(1)
    await pool.release(again)
  })
})

describe('GraphHostWorktreePool (real git)', () => {
  it('mints and re-adopts real worktree slots through the git service', async () => {
    const dir = realpathSync(mkdtempSync(join(tmpdir(), 'dsh-host-wt-')))
    const poolRootSetting = { root: join(dir, '.graph-worktrees') }
    gitRun(dir, ['init', '-q', '-b', 'master'])
    gitRun(dir, ['config', 'user.email', 'test@example.com'])
    gitRun(dir, ['config', 'user.name', 'Test User'])
    gitRun(dir, ['config', 'commit.gpgsign', 'false'])
    await writeFile(join(dir, 'seed.txt'), 'seed\n')
    gitRun(dir, ['add', 'seed.txt'])
    gitRun(dir, ['commit', '-qm', 'init'])

    const ctx = new Context()
    await ctx.plugin(LocalSubprocessRuntime)
    const git = new GitService(ctx)
    const worktrees: GraphHostWorktrees = {
      repoRoot: dir,
      async acquire(options) {
        const lease = await acquireWorktree(git, dir, poolRootSetting, {
          holder: options.holder,
          ...(options.branch !== undefined
            ? { branch: options.branch }
            : {}),
        })
        return { leaseId: lease.leaseId, path: lease.path, repoRoot: dir }
      },
      async list() {
        const entries = await listWorktrees(git, dir, { settings: poolRootSetting })
        return entries.map(entry => ({
          name: entry.name,
          path: entry.path,
          ...(entry.branch !== undefined
            ? { branch: entry.branch }
            : {}),
          ...(entry.leaseHolder !== undefined
            ? { leaseHolder: entry.leaseHolder }
            : {}),
          leased: entry.leased,
          exists: entry.exists,
        }))
      },
    }
    const pool = new GraphHostWorktreePool(worktrees)
    const lease = await pool.acquire('graph_operator_lease_abc')
    expect(lease.path).toContain('.graph-worktrees')
    expect(await readFile(join(lease.path, 'seed.txt'), 'utf8')).toBe('seed\n')
    expect(gitRun(lease.path, ['rev-parse', '--abbrev-ref', 'HEAD'])).toBe('graph_operator_lease_abc')

    // Same key re-adopts the minted slot; a different key mints a second one.
    const again = await pool.acquire('graph_operator_lease_abc')
    expect(again.path).toBe(lease.path)
    const other = await pool.acquire('graph_operator_lease_def')
    expect(other.path).not.toBe(lease.path)
    await pool.release(lease)
    await pool.release(other)
    await ctx.fiber.dispose()
    rmSync(dir, { recursive: true, force: true })
  })
})

function gitRun(cwd: string, args: string[]): string {
  const result = spawnSync('git', args, { cwd, encoding: 'utf8' })
  if (result.status !== 0) {
    throw new Error(
      `git ${args.join(' ')} failed (exit ${result.status}): ${result.stderr.trim()}`,
    )
  }
  return result.stdout.trim()
}

/* --------------------------- host assembly ---------------------------- */

describe('graph host assembly', () => {
  it('provisions, starts one operator child, folds the terminal record, and emits graph/change', async () => {
    const harness = await makeHarness({ attach: true })
    await harness.services.controller.schedule(GRAPH, scheduleRequest({ addWork: [work('w1')] }))

    await waitUntil(() => harness.subagents.starts.length === 1)
    const start = must(harness.subagents.starts[0])
    expect(start.request.prompt).toEqual([{ type: 'text', text: 'do w1' }])
    expect((start.request as { workspace?: string }).workspace).toMatch(/^\/worktrees\/graph_operator_lease_/)
    expect(start.request.parent).toBeDefined()

    const producedKey = harness.worktrees.acquireCalls[0]?.holder ?? ''
    expect(producedKey.startsWith('graph_operator_lease_')).toBe(true)

    const run = must(harness.subagents.runs[0])
    run.settle('completed', [{ type: 'text', text: 'finished building views' }])

    // The record arrives through the sink; the next drive folds + emits.
    const coordinator = harness.services.controller.getOrCreate(GRAPH)
    await coordinator.reconcileAndWait()

    const snapshot = await harness.services.snapshotFor(GRAPH)
    expect(snapshot.work).toHaveLength(1)
    const work0 = must(snapshot.work[0])
    expect(work0.workId).toBe('w1')
    expect(work0.status).toBe('finished')
    expect(work0.inputCount).toBe(0)
    expect(work0.operatorId).toBeDefined()

    const lastEvent = harness.sessionEvents.events.at(-1)
    expect(lastEvent).toBeDefined()
    expect(lastEvent?.data.graphId).toBe(GRAPH)
    expect(lastEvent?.data.snapshot.work[0]?.status).toBe('finished')

    await harness.services.dispose()
    await harness.backend.close()
    cleanup(harness.path)
  })

  it('never re-runs a finished activation on a re-drive (idempotent executor guard)', async () => {
    const harness = await makeHarness({ attach: true })
    await harness.services.controller.schedule(GRAPH, scheduleRequest({ addWork: [work('w1')] }))
    await waitUntil(() => harness.subagents.runs.length === 1)
    must(harness.subagents.runs[0]).settle('completed')

    const coordinator = harness.services.controller.getOrCreate(GRAPH)
    await coordinator.reconcileAndWait()
    await coordinator.reconcileAndWait()
    expect(harness.subagents.starts).toHaveLength(1)

    await harness.services.dispose()
    await harness.backend.close()
    cleanup(harness.path)
  })

  it('serializes activations of one operator and attributes each terminal record', async () => {
    const harness = await makeHarness({ attach: false })
    const store = harness.services.store
    const provision: AgentGraphOperatorProvisionRequest = {
      provisionId: 'graph_provision_1',
      graphId: GRAPH,
      workId: 'w1',
      operatorId: 'op-x',
      targetSessionId: 'graph_session_x',
      initialTurnId: 'turn-1',
      initialRunId: 'run-1',
      provisionFingerprint: 'fp-1',
      edges: [],
      expectedScheduleRevision: 0,
    }
    await store.provisionOperator(provision)

    const claimRequest = (intentId: string, runId: string): AgentGraphIntentClaimRequest => ({
      schemaVersion: AGENT_GRAPH_INTENT_CLAIM_SCHEMA_VERSION,
      claimId: `claim-${intentId}`,
      graphId: GRAPH,
      intentId,
      intentFingerprint: `intent-fp-${intentId}`,
      readinessContextFingerprint: 'rc-1',
      targetOperatorId: 'op-x',
      targetSessionId: 'graph_session_x',
      targetTurnId: `turn-${runId}`,
      targetRunId: runId,
    })
    const claimOne = (await store.claimIntent(claimRequest('intent-a', 'run-a'))).claim
    const claimTwo = (await store.claimIntent(claimRequest('intent-b', 'run-b'))).claim

    const inputFor = (claim: typeof claimOne): AgentGraphRunClaimedIntentInput => ({
      intent: {
        schemaVersion: 1,
        intentId: claim.intentId,
        graphId: GRAPH,
        readinessContextFingerprint: 'rc-1',
        policyFingerprint: 'pf-1',
        readinessId: 'w1',
        operatorId: 'op-x',
        targetSessionId: 'graph_session_x',
        inputIds: [],
        selectedResultInputs: [],
        policyKind: 'supervisor',
        triggerRouteIds: [],
        triggerRecordIds: [],
      },
      claim,
      prompt: 'run one intent',
    })

    const executor = harness.services.executor
    const provisioned = await executor.provisionOperator(provision)
    expect(provisioned).toBeDefined()
    const first = executor.runClaimedAgentGraphIntent(inputFor(claimOne))
    await waitUntil(() => harness.subagents.runs.length === 1)
    expect(harness.subagents.runs).toHaveLength(1)
    const second = executor.runClaimedAgentGraphIntent(inputFor(claimTwo))
    await new Promise(resolve => setTimeout(resolve, 50))
    expect(harness.subagents.runs).toHaveLength(1)

    must(harness.subagents.runs[0]).settle('completed', [{ type: 'text', text: 'first done' }])
    await first
    await waitUntil(() => harness.subagents.runs.length === 2)
    must(harness.subagents.runs[1]).settle('completed', [{ type: 'text', text: 'second done' }])
    await second
    expect(harness.subagents.starts).toHaveLength(2)

    const events = await harness.services.store
      .snapshot()
      .then(() => harness.services.controller.snapshot(GRAPH))
    expect(events.records).toHaveLength(2)

    await harness.services.dispose()
    await harness.backend.close()
    cleanup(harness.path)
  })

  it('delivers a wake: re-drives, emits a fresh graph/change, settles the wake', async () => {
    const harness = await makeHarness({ attach: true })
    await harness.services.controller.schedule(GRAPH, scheduleRequest({ addWork: [work('w1')] }))
    await waitUntil(() => harness.subagents.runs.length === 1)
    must(harness.subagents.runs[0]).settle('completed', [{ type: 'text', text: 'done' }])
    await harness.services.controller.getOrCreate(GRAPH).reconcileAndWait()

    const yielded = await harness.services.controller.yield(GRAPH)
    const before = harness.sessionEvents.events.length
    await harness.services.wakeRuntime.handleIdle(ROOT)

    const wake = await harness.services.store.readSupervisorWake(GRAPH, yielded.wakeId)
    expect(wake?.status).toBe('delivered')
    expect(harness.sessionEvents.events.length).toBeGreaterThan(before)

    await harness.services.dispose()
    await harness.backend.close()
    cleanup(harness.path)
  })

  it('supersedes a wake when the graph is finished without calling deliver', async () => {
    const harness = await makeHarness({ attach: true })
    await harness.services.controller.schedule(GRAPH, scheduleRequest({ addWork: [work('w1')] }))
    await waitUntil(() => harness.subagents.runs.length === 1)
    must(harness.subagents.runs[0]).settle('completed')
    await harness.services.controller.getOrCreate(GRAPH).reconcileAndWait()

    const yielded = await harness.services.controller.yield(GRAPH)
    const beforeFinish = harness.sessionEvents.events.length
    await harness.services.controller.schedule(GRAPH, scheduleRequest({ finish: { resultIds: [], reason: 'all done' } }))
    // The finish is a schedule commit whose emission is async (fire-and-forget);
    // wait for it to land so the wake-attempt accounting below is deterministic.
    await waitUntil(() => harness.sessionEvents.events.length > beforeFinish)
    const emitsBefore = harness.sessionEvents.events.length
    await harness.services.wakeRuntime.handleIdle(ROOT)

    const wake = await harness.services.store.readSupervisorWake(GRAPH, yielded.wakeId)
    expect(wake?.status).toBe('superseded')
    // The finish emission happened at the commit; the wake itself never re-drove.
    expect(harness.sessionEvents.events.length).toBe(emitsBefore)

    await harness.services.dispose()
    await harness.backend.close()
    cleanup(harness.path)
  })

  it('overflow: compacts once, then carries a bounded partial and exhausts the wake', async () => {
    const harness = await makeHarness({ attach: true })
    await harness.services.controller.schedule(GRAPH, scheduleRequest({ addWork: [work('w1')] }))
    await waitUntil(() => harness.subagents.runs.length === 1)
    must(harness.subagents.runs[0]).settle('completed')
    await harness.services.controller.getOrCreate(GRAPH).reconcileAndWait()
    const yielded = await harness.services.controller.yield(GRAPH)

    const store = harness.services.store
    const original = store.listScheduleUpdates.bind(store)
    const overflow = new GraphHostContextOverflowError('agent context overflowed')
    const patched = store as { listScheduleUpdates: typeof store.listScheduleUpdates }
    patched.listScheduleUpdates = async () => {
      throw overflow
    }

    await harness.services.wakeRuntime.handleIdle(ROOT)
    expect(harness.compaction.requests).toHaveLength(1)

    patched.listScheduleUpdates = original
    const wake = await harness.services.store.readSupervisorWake(GRAPH, yielded.wakeId)
    expect(['retryable_failed', 'superseded']).toContain(wake?.status)

    await harness.services.dispose()
    await harness.backend.close()
    cleanup(harness.path)
  })

  it('dispose stops deliveries and closes the store', async () => {
    const harness = await makeHarness({ attach: true })
    await harness.services.dispose()
    await expect(harness.services.wakeRuntime.handleIdle(ROOT)).resolves.toBeUndefined()
    // The control store serves reads from its in-memory mirror, so closing the
    // durable medium rejects the next write rather than the reads.
    await expect(
      harness.services.store.commitScheduleUpdate(
        scheduleRequest({ addWork: [work('w2')] }),
      ),
    ).rejects.toThrow()
    await harness.backend.close()
    cleanup(harness.path)
  })
})

/* ------------------------------ plugin -------------------------------- */

/** Minimal cordis context exposing only what `apply` touches. */
class StubContext {
  readonly provided = new Map<string, unknown>()
  readonly listeners = new Map<string, Set<(...args: unknown[]) => void>>()
  readonly cleanups: (() => void | Promise<void>)[] = []
  readonly agents: { get: (id: SessionId) => Agent | undefined }
  readonly sessions: { get: (id: SessionId) => Session | undefined }
  readonly subagents: { start: () => Promise<never> }
  readonly git: GitService
  readonly compaction: { compactNow: () => Promise<never> }
  private readonly values = new Map<string, unknown>()

  constructor(options: {
    rootAgent?: Agent
    values?: Record<string, unknown>
  }) {
    const rootAgent = options.rootAgent
    this.agents = { get: id => (String(id) === ROOT ? rootAgent : undefined) }
    this.sessions = { get: () => undefined }
    this.subagents = {
      start: async () => {
        throw new Error('stub: no subagent start in the smoke test')
      },
    }
    this.compaction = {
      compactNow: async () => {
        throw new Error('stub: no compaction in the smoke test')
      },
    }
    this.git = {
      run: async (args: readonly string[]): Promise<{ exitCode: number; stdout: string; stderr: string }> => {
        const argv = [...args]
        if (argv.includes('--is-inside-work-tree')) {
          return { exitCode: 0, stdout: 'true', stderr: '' }
        }
        return { exitCode: 0, stdout: join(process.cwd(), '.git'), stderr: '' }
      },
    } as unknown as GitService
    for (const [key, value] of Object.entries(options.values ?? {})) {
      this.values.set(key, value)
    }
  }

  on(name: string, listener: (...args: unknown[]) => void): () => void {
    const listeners = this.listeners.get(name) ?? new Set()
    listeners.add(listener)
    this.listeners.set(name, listeners)
    return () => {
      listeners.delete(listener)
    }
  }

  effect(
    callback: () => (() => void | Promise<void>) | undefined,
    _name?: string,
  ): () => void {
    const cleanup = callback()
    if (cleanup !== undefined) this.cleanups.push(cleanup)
    return () => undefined
  }

  provide(name: string, value: unknown): void {
    this.provided.set(name, value)
  }

  get(name: string): unknown {
    return this.values.get(name)
  }

  async dispose(): Promise<void> {
    for (const cleanup of [...this.cleanups].reverse()) await cleanup()
    this.cleanups.length = 0
  }
}

function stubAgent(): Agent {
  const ctx = {
    on: () => () => undefined,
    effect: (callback: () => void) => {
      callback()
      return () => undefined
    },
  }
  return {
    id: SessionId(ROOT),
    ctx,
    options: {},
    session: { append: () => undefined },
  } as unknown as Agent
}

describe('graph-host plugin', () => {
  it('provides the controller + services when the root agent is already live', async () => {
    const path = tmpPath()
    const backend = new SqliteStorageBackend(new Config({ path }))
    const ctx = new StubContext({
      rootAgent: stubAgent(),
      values: {
        'storage.backend.sqlite': backend,
      },
    })
    apply(ctx as unknown as Context, {
      rootSessionId: ROOT,
      subagentProvider: 'fake',
    })
    await waitUntil(() => ctx.provided.get(SERVICE_AGENT_GRAPH_CONTROLLER) !== undefined)
    expect(ctx.provided.get(SERVICE_AGENT_GRAPH_CONTROLLER)).toBeDefined()
    expect(ctx.provided.get(SERVICE_GRAPH_HOST)).toBeDefined()
    await ctx.dispose()
    await backend.close()
    cleanup(path)
  })

  it('builds only once and disposes the assembly with the fiber', async () => {
    const path = tmpPath()
    const backend = new SqliteStorageBackend(new Config({ path }))
    const ctx = new StubContext({
      rootAgent: stubAgent(),
      values: {
        'storage.backend.sqlite': backend,
      },
    })
    apply(ctx as unknown as Context, {
      rootSessionId: ROOT,
      subagentProvider: 'fake',
    })
    await waitUntil(() => ctx.provided.get(SERVICE_GRAPH_HOST) !== undefined)
    expect(ctx.provided.get(SERVICE_GRAPH_HOST)).toBeDefined()
    // A second build attempt is a no-op (already built).
    await ctx.dispose()
    await backend.close()
    cleanup(path)
  })
})

/* ------------------------------ projection ---------------------------- */

describe('host projection bounds', () => {
  it('caps instruction length at 300 characters', async () => {
    const harness = await makeHarness({ attach: true })
    await harness.services.controller.schedule(
      GRAPH,
      scheduleRequest({ addWork: [work('w1', { instruction: 'y'.repeat(400) })] }),
    )
    await waitUntil(() => harness.subagents.runs.length === 1)
    must(harness.subagents.runs[0]).settle('completed')
    await harness.services.controller.getOrCreate(GRAPH).reconcileAndWait()
    const snapshot = await harness.services.snapshotFor(GRAPH)
    const boundWork = must(snapshot.work[0])
    expect(boundWork.instruction.length).toBeLessThanOrEqual(300)
    expect(boundWork.instruction.endsWith('…')).toBe(true)
    await harness.services.dispose()
    await harness.backend.close()
    cleanup(harness.path)
  })
})

