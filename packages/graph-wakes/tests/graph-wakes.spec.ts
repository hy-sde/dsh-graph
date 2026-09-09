import { afterEach, describe, expect, it, vi } from 'vitest'
import { Config, SqliteStorageBackend } from '@deepseek-ai/dsh-storage-sqlite'
import { GraphControlStore, graphWakeAttemptId, graphWakeId } from '@hy-sde-org/dsh-graph-control'
import type { AgentGraphSupervisorWakeRecord } from '@hy-sde-org/dsh-graph-control'
import { GraphWakeRuntime } from '../src/runtime.ts'
import type {
  GraphWakeDeliver,
  GraphWakeDeliveryOutcome,
  GraphWakeDue,
  GraphWakeIdleCallback,
  GraphWakeRuntimeOptions,
} from '../src/types.ts'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const ROOT = 'root-1'
const ROOT_2 = 'root-2'
const GRAPH = 'graph_g1'
const SNAPSHOT = 'snap-1'
const WAKE_ID = graphWakeId(GRAPH, SNAPSHOT)

/* ------------------------------ fixtures ------------------------------ */

const tmpDirs: string[] = []
const runtimes: GraphWakeRuntime[] = []
let seq = 0

function tmpPath(): string {
  const dir = mkdtempSync(join(tmpdir(), 'graph-wakes-'))
  tmpDirs.push(dir)
  return join(dir, 'test.db')
}

async function openStore(path: string): Promise<GraphControlStore> {
  const backend = new SqliteStorageBackend(new Config({ path }))
  const unit = await backend.kv.open(GraphControlStore.descriptor)
  return GraphControlStore.open(unit)
}

function must<T>(value: T | undefined): T {
  if (value === undefined) throw new Error('expected a defined value')
  return value
}

async function claimWake(
  store: GraphControlStore,
  overrides: Partial<{ graphId: string; wakeId: string; snapshotVersion: string; rootSessionId: string }> = {},
): Promise<AgentGraphSupervisorWakeRecord> {
  const graphId = overrides.graphId ?? GRAPH
  const snapshotVersion = overrides.snapshotVersion ?? SNAPSHOT
  const wakeId = overrides.wakeId ?? graphWakeId(graphId, snapshotVersion)
  const rootSessionId = overrides.rootSessionId ?? ROOT
  const { wake } = await store.claimSupervisorWake({ graphId, wakeId, snapshotVersion, rootSessionId })
  return wake
}

function updateRequest(
  overrides: { stop?: readonly { targetId: string; reason: string }[]; finish?: { resultIds: readonly string[]; reason: string } } = {},
): Parameters<GraphControlStore['commitScheduleUpdate']>[0] {
  seq += 1
  return {
    schemaVersion: 1,
    updateId: `graph_update_${seq}`,
    updateFingerprint: `fp-${seq}`,
    graphId: GRAPH,
    source: { sessionId: ROOT, runId: `run-${seq}`, turnId: `turn-${seq}`, toolCallId: `call-${seq}` },
    addWork: [],
    stop: overrides.stop ?? [],
    ...(overrides.finish !== undefined ? { finish: overrides.finish } : {}),
  }
}

function makeClock(start = 1_000_000): { now: () => number; advance: (ms: number) => void } {
  let value = start
  return {
    now: () => value,
    advance: (ms: number) => {
      value += ms
    },
  }
}

function makeDeliver(
  script: Array<GraphWakeDeliveryOutcome | ((due: GraphWakeDue) => GraphWakeDeliveryOutcome)> = [],
): { deliver: GraphWakeDeliver; calls: GraphWakeDue[] } {
  const calls: GraphWakeDue[] = []
  const deliver: GraphWakeDeliver = async (due) => {
    calls.push(due)
    const next = script.shift()
    if (next === undefined) return { kind: 'delivered' }
    return typeof next === 'function' ? next(due) : next
  }
  return { deliver, calls }
}

function makeProbe(): {
  observeIdle: (onIdle: GraphWakeIdleCallback) => () => void
  emit: (sessionId: string) => void
  active: () => boolean
} {
  let callback: GraphWakeIdleCallback | undefined
  let subscribed = false
  return {
    observeIdle: (onIdle) => {
      callback = onIdle
      subscribed = true
      return () => {
        subscribed = false
        callback = undefined
      }
    },
    emit: sessionId => callback?.(sessionId),
    active: () => subscribed,
  }
}

function makeRuntime(
  store: GraphControlStore,
  deliver: GraphWakeDeliver,
  overrides: Partial<GraphWakeRuntimeOptions> = {},
): GraphWakeRuntime {
  const runtime = new GraphWakeRuntime({ store, deliver, ...overrides })
  runtimes.push(runtime)
  return runtime
}

async function waitUntil(check: () => boolean | Promise<boolean>): Promise<void> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    if (await check()) return
    await new Promise<void>((resolve) => {
      setTimeout(() => {
        resolve()
      }, 5)
    })
  }
  throw new Error('condition not reached')
}

afterEach(async () => {
  for (const runtime of runtimes.splice(0)) await runtime.stop()
  for (const dir of tmpDirs.splice(0)) rmSync(dir, { recursive: true, force: true })
  seq = 0
})

/* ------------------------------ runtime ------------------------------- */

describe('GraphWakeRuntime', () => {
  it('delivers a pending wake only after an idle signal', async () => {
    const store = await openStore(tmpPath())
    await claimWake(store)
    const { deliver, calls } = makeDeliver()
    const runtime = makeRuntime(store, deliver, { now: () => 0 })

    expect(await runtime.pendingWakes()).toHaveLength(1)
    expect(calls).toHaveLength(0)

    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(1)
    expect(calls[0]).toEqual({ graphId: GRAPH, wakeId: WAKE_ID, rootSessionId: ROOT, snapshotVersion: SNAPSHOT })

    const wake = must(await store.readSupervisorWake(GRAPH, WAKE_ID))
    expect(wake.status).toBe('delivered')
    expect(wake.attemptCount).toBe(1)
    const attempts = await store.listSupervisorWakeAttempts(GRAPH, WAKE_ID)
    expect(attempts).toHaveLength(1)
    expect(attempts[0]).toMatchObject({
      status: 'delivered',
      attemptId: graphWakeAttemptId(WAKE_ID, 1),
      turnId: graphWakeAttemptId(WAKE_ID, 1),
    })
    expect(await runtime.pendingWakes()).toHaveLength(0)
    expect(await runtime.wakeStatus(WAKE_ID)).toMatchObject({ status: 'delivered', attemptCount: 1 })
  })

  it('delivers through the injected idle observer, scoped to the started root', async () => {
    const store = await openStore(tmpPath())
    await claimWake(store)
    await claimWake(store, { rootSessionId: ROOT_2, snapshotVersion: 'snap-2' })
    const probe = makeProbe()
    const { deliver, calls } = makeDeliver()
    const runtime = makeRuntime(store, deliver, { observeIdle: probe.observeIdle })
    runtime.start(ROOT)
    expect(probe.active()).toBe(true)

    probe.emit(ROOT_2)
    await new Promise<void>((resolve) => {
      setTimeout(() => {
        resolve()
      }, 20)
    })
    expect(calls).toHaveLength(0)

    probe.emit(ROOT)
    await waitUntil(() => calls.length > 0)
    expect(calls).toHaveLength(1)
    expect(calls[0]?.rootSessionId).toBe(ROOT)
    expect(must(await store.readSupervisorWake(GRAPH, WAKE_ID)).status).toBe('delivered')
  })

  it('re-arms retryable failures with backoff up to maxAttempts, then stops', async () => {
    const store = await openStore(tmpPath())
    await claimWake(store)
    const clock = makeClock()
    const { deliver, calls } = makeDeliver([
      { kind: 'retryable_failed', failureReason: 'boom-1' },
      { kind: 'retryable_failed', failureReason: 'boom-2' },
      { kind: 'retryable_failed', failureReason: 'boom-3' },
    ])
    const runtime = makeRuntime(store, deliver, { now: clock.now })

    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(1)
    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(1)

    clock.advance(30_000)
    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(2)
    clock.advance(60_000)
    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(3)

    const wake = must(await store.readSupervisorWake(GRAPH, WAKE_ID))
    expect(wake.status).toBe('retryable_failed')
    expect(wake.attemptCount).toBe(3)
    const attempts = await store.listSupervisorWakeAttempts(GRAPH, WAKE_ID)
    expect(attempts.map(attempt => attempt.failureReason)).toEqual(['boom-1', 'boom-2', 'boom-3'])

    clock.advance(1_000_000)
    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(3)
  })

  it('recovers overflow with exactly one compact and one bounded partial delivery', async () => {
    const store = await openStore(tmpPath())
    await claimWake(store)
    const onCompact = vi.fn(async () => {})
    const { deliver, calls } = makeDeliver([
      { kind: 'retryable_failed', overflow: true, failureReason: 'context overflow' },
      { kind: 'delivered', partialResult: true },
    ])
    const runtime = makeRuntime(store, deliver, { onCompact, now: () => 0 })

    await runtime.handleIdle(ROOT)
    expect(onCompact).toHaveBeenCalledTimes(1)
    expect(onCompact).toHaveBeenCalledWith(ROOT)
    expect(calls).toHaveLength(1)

    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(2)
    expect(onCompact).toHaveBeenCalledTimes(1)
    expect(must(await store.readSupervisorWake(GRAPH, WAKE_ID)).status).toBe('delivered')
    expect(must(await store.readSupervisorWake(GRAPH, WAKE_ID)).attemptCount).toBe(2)

    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(2)
    expect(onCompact).toHaveBeenCalledTimes(1)
  })

  it('refuses a third identical full delivery after the partial attempt overflows', async () => {
    const store = await openStore(tmpPath())
    await claimWake(store)
    const onCompact = vi.fn(async () => {})
    const { deliver, calls } = makeDeliver([
      { kind: 'retryable_failed', overflow: true, failureReason: 'overflow-1' },
      { kind: 'retryable_failed', overflow: true, partialResult: true, failureReason: 'overflow-2' },
    ])
    const runtime = makeRuntime(store, deliver, { onCompact, now: () => 0 })

    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(1)
    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(2)
    expect(onCompact).toHaveBeenCalledTimes(1)

    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(2)
    expect(onCompact).toHaveBeenCalledTimes(1)
    expect(must(await store.readSupervisorWake(GRAPH, WAKE_ID))).toMatchObject({
      status: 'retryable_failed',
      attemptCount: 2,
    })
  })

  it('terminates an overflowing wake when no compact hook is wired', async () => {
    const store = await openStore(tmpPath())
    await claimWake(store)
    const { deliver, calls } = makeDeliver([{ kind: 'retryable_failed', overflow: true, failureReason: 'overflow' }])
    const runtime = makeRuntime(store, deliver, { now: () => 0 })

    await runtime.handleIdle(ROOT)
    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(1)
    expect(must(await store.readSupervisorWake(GRAPH, WAKE_ID))).toMatchObject({
      status: 'retryable_failed',
      attemptCount: 1,
    })
  })

  it('parks waiting_permission and never re-attempts it on later idle signals', async () => {
    const store = await openStore(tmpPath())
    await claimWake(store)
    const { deliver, calls } = makeDeliver([{ kind: 'waiting_permission' }])
    const runtime = makeRuntime(store, deliver, { now: () => 0 })

    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(1)
    expect(must(await store.readSupervisorWake(GRAPH, WAKE_ID))).toMatchObject({
      status: 'waiting_permission',
      attemptCount: 1,
    })

    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(1)
    expect(must(await store.readSupervisorWake(GRAPH, WAKE_ID))).toMatchObject({
      status: 'waiting_permission',
      attemptCount: 1,
    })
  })

  it('supersedes without delivery when the schedule log stops the root or graph', async () => {
    const store = await openStore(tmpPath())
    await claimWake(store)
    await store.commitScheduleUpdate(updateRequest({ stop: [{ targetId: ROOT, reason: 'user stop' }] }))
    const { deliver, calls } = makeDeliver()
    const runtime = makeRuntime(store, deliver, { now: () => 0 })

    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(0)
    expect(must(await store.readSupervisorWake(GRAPH, WAKE_ID))).toMatchObject({
      status: 'superseded',
      supersededReason: 'agent_graph_stopped',
    })
  })

  it('supersedes without delivery when the graph is closed (finish update)', async () => {
    const store = await openStore(tmpPath())
    await claimWake(store)
    await store.commitScheduleUpdate(updateRequest({ finish: { resultIds: ['record-1'], reason: 'done' } }))
    const { deliver, calls } = makeDeliver()
    const runtime = makeRuntime(store, deliver, { now: () => 0 })

    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(0)
    expect(must(await store.readSupervisorWake(GRAPH, WAKE_ID)).status).toBe('superseded')
  })

  it('does not suppress wakes for work-item stops', async () => {
    const store = await openStore(tmpPath())
    await claimWake(store)
    await store.commitScheduleUpdate(updateRequest({ stop: [{ targetId: 'work-1', reason: 'work stop' }] }))
    const { deliver, calls } = makeDeliver()
    const runtime = makeRuntime(store, deliver, { now: () => 0 })

    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(1)
    expect(must(await store.readSupervisorWake(GRAPH, WAKE_ID)).status).toBe('delivered')
  })

  it('stop suppresses later idles; a fresh runtime restarts against the same rows', async () => {
    const store = await openStore(tmpPath())
    await claimWake(store)
    const probe = makeProbe()
    const { deliver, calls } = makeDeliver()
    const runtime = makeRuntime(store, deliver, { observeIdle: probe.observeIdle })
    runtime.start(ROOT)
    await runtime.stop()
    expect(probe.active()).toBe(false)

    probe.emit(ROOT)
    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(0)

    const clock = makeClock()
    const fresh = makeRuntime(store, deliver, { now: clock.now })
    expect(await fresh.pendingWakes()).toHaveLength(1)
    await fresh.handleIdle(ROOT)
    expect(calls).toHaveLength(1)
    expect(must(await store.readSupervisorWake(GRAPH, WAKE_ID)).status).toBe('delivered')
  })

  it('coalesces overlapping idle signals into one delivery', async () => {
    const store = await openStore(tmpPath())
    await claimWake(store)
    const gate = Promise.withResolvers<undefined>()
    const calls: GraphWakeDue[] = []
    const deliver: GraphWakeDeliver = async (due) => {
      calls.push(due)
      await gate.promise
      return { kind: 'delivered' }
    }
    const runtime = makeRuntime(store, deliver, { now: () => 0 })

    const idle = runtime.handleIdle(ROOT)
    const overlapping = runtime.handleIdle(ROOT)
    gate.resolve(undefined)
    await Promise.all([idle, overlapping])
    expect(calls).toHaveLength(1)
  })

  it('refuses to deliver when the attempt row is already owned', async () => {
    const store = await openStore(tmpPath())
    await claimWake(store)
    await store.beginSupervisorWakeAttempt({
      graphId: GRAPH,
      wakeId: WAKE_ID,
      attemptId: graphWakeAttemptId(WAKE_ID, 1),
      turnId: 'turn-owned',
    })
    const { deliver, calls } = makeDeliver()
    const runtime = makeRuntime(store, deliver, { now: () => 0 })

    await runtime.handleIdle(ROOT)
    expect(calls).toHaveLength(0)
    expect(must(await store.readSupervisorWake(GRAPH, WAKE_ID)).status).toBe('running')
  })

  it('marks a throwing deliver hook retryable and re-arms it', async () => {
    const store = await openStore(tmpPath())
    await claimWake(store)
    const clock = makeClock()
    let failures = 0
    const deliver: GraphWakeDeliver = async () => {
      failures += 1
      throw new Error(`hook failure ${failures}`)
    }
    const runtime = makeRuntime(store, deliver, { now: clock.now })

    await runtime.handleIdle(ROOT)
    expect(must(await store.readSupervisorWake(GRAPH, WAKE_ID))).toMatchObject({
      status: 'retryable_failed',
      attemptCount: 1,
    })
    const attempts = await store.listSupervisorWakeAttempts(GRAPH, WAKE_ID)
    expect(attempts[0]?.failureReason).toBe('hook failure 1')

    clock.advance(30_000)
    await runtime.handleIdle(ROOT)
    expect(must(await store.readSupervisorWake(GRAPH, WAKE_ID)).attemptCount).toBe(2)
  })
})
