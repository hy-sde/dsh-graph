import { afterEach, describe, expect, it } from 'vitest'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Config, SqliteStorageBackend } from '@deepseek-ai/dsh-storage-sqlite'
import { GraphControlStore } from '../src/index.ts'
import type { AgentGraphOperatorBinding } from '../src/index.ts'

/** Operator worktree bindings: keyed by provisionId, index by graphId:workId, lease CAS on rebind. */

const dirs: string[] = []
afterEach(async () => {
  for (const dir of dirs.splice(0)) await rm(dir, { recursive: true, force: true })
})

async function freshPath(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'dsh-graph-bindings-'))
  dirs.push(dir)
  return join(dir, 'graph.db')
}

async function openStore(path: string): Promise<GraphControlStore> {
  const backend = new SqliteStorageBackend(new Config({ path }))
  const unit = await backend.kv.open(GraphControlStore.descriptor)
  return GraphControlStore.open(unit)
}

const GRAPH = 'graph_session_1'

let seq = 0
function binding(overrides: Partial<AgentGraphOperatorBinding> = {}): AgentGraphOperatorBinding {
  seq += 1
  return {
    graphId: GRAPH,
    workId: `graph_work_${seq}`,
    provisionId: `graph_provision_${seq}`,
    leaseId: `lease-${seq}`,
    path: `/tmp/pool/${seq}`,
    repoRoot: '/tmp/repo',
    boundAt: seq,
    ...overrides,
  }
}

describe('operator worktree bindings', () => {
  it('binds, reads, and lists by graph (and globally)', async () => {
    const store = await openStore(await freshPath())
    const first = binding()
    const second = binding({ graphId: 'other_graph' })
    await store.bindOperatorWorktree(first)
    await store.bindOperatorWorktree(second)

    expect(await store.readOperatorBinding(first.provisionId)).toEqual(first)
    expect(await store.readOperatorBindingByWork(GRAPH, first.workId)).toEqual(first)
    expect(await store.listOperatorBindings(GRAPH)).toEqual([first])
    expect(await store.listOperatorBindings()).toHaveLength(2)
    await store.close()
  })

  it('adopts a same-lease rebind idempotently and keeps the original boundAt', async () => {
    const store = await openStore(await freshPath())
    const first = binding({ boundAt: 100 })
    await store.bindOperatorWorktree(first)
    await store.bindOperatorWorktree({ ...first, path: '/tmp/pool/updated', boundAt: 200 })

    const stored = await store.readOperatorBinding(first.provisionId)
    expect(stored?.leaseId).toBe(first.leaseId)
    expect(stored?.path).toBe('/tmp/pool/updated')
    expect(stored?.boundAt).toBe(100)
    expect(await store.listOperatorBindings(GRAPH)).toHaveLength(1)
    await store.close()
  })

  it('rejects re-binding the same provision with a different lease id', async () => {
    const store = await openStore(await freshPath())
    const first = binding()
    await store.bindOperatorWorktree(first)
    await expect(
      store.bindOperatorWorktree({ ...first, leaseId: 'lease-other', boundAt: 200 }),
    ).rejects.toMatchObject({ code: 'binding-conflict' })
    expect((await store.readOperatorBinding(first.provisionId))?.leaseId).toBe(first.leaseId)
    await store.close()
  })

  it('survives reopen: rows and the derived index heal from the authoritative table', async () => {
    const path = await freshPath()
    const store = await openStore(path)
    const first = binding()
    const second = binding()
    await store.bindOperatorWorktree(first)
    await store.bindOperatorWorktree(second)
    await store.close()

    const reopened = await openStore(path)
    expect(await reopened.readOperatorBinding(first.provisionId)).toEqual(first)
    expect(await reopened.readOperatorBindingByWork(GRAPH, first.workId)).toEqual(first)
    expect(await reopened.listOperatorBindings(GRAPH)).toHaveLength(2)
    const snapshot = await reopened.snapshot()
    expect(snapshot.operatorBindings).toHaveLength(2)
    await reopened.close()
  })
})
