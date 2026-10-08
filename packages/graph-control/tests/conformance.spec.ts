import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { Config, SqliteStorageBackend } from '@deepseek-ai/dsh-storage-sqlite'
import {
  assertGraphControlStoreConformance,
  graphControlStoreConformanceChecks,
  GraphControlStore,
} from '../src/index.ts'

/* ------------------------------ fixtures ------------------------------- */

const dirs: string[] = []
const backends: SqliteStorageBackend[] = []
let seq = 0

afterEach(() => {
  for (const backend of backends.splice(0)) void backend.close()
  for (const dir of dirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true })
  }
})

/** Fresh sqlite-backed store per check, tracked for teardown. */
function openSqliteStore(): Promise<GraphControlStore> {
  const dir = dirs[dirs.length - 1]
  if (dir === undefined) throw new Error('conformance dir not prepared')
  seq += 1
  const backend = new SqliteStorageBackend(
    new Config({ path: join(dir, `conformance-${seq}.db`) }),
  )
  backends.push(backend)
  return backend.kv.open(GraphControlStore.descriptor).then(unit =>
    GraphControlStore.open(unit),
  )
}

/* ------------------------------- battery ------------------------------- */

describe('graph-control store conformance', () => {
  it('passes the full battery against a sqlite-backed store', async () => {
    dirs.push(mkdtempSync(join(tmpdir(), 'graph-control-conformance-')))
    await assertGraphControlStoreConformance(openSqliteStore)
  })

  it('exposes the complete named check list', () => {
    const names = graphControlStoreConformanceChecks().map(check => check.name)
    expect(names).toHaveLength(16)
    expect(new Set(names).size).toBe(16)
  })
})
