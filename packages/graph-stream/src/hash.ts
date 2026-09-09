/**
 * Canonical stable hashing (Maka `request-shape.stableHash`): ids in the
 * graph are byte-stable across processes and restarts. Canonicalization:
 * object keys sorted, undefined/function/symbol kept as explicit markers,
 * bigint stringified, `required`/`enum` array members sorted, Dates as ISO.
 * @module
 */

import { createHash } from 'node:crypto'
export function stableStringify(value: unknown): string {
  return JSON.stringify(canonicalize(value))
}

export function stableHash(value: unknown): `sha256:${string}` {
  return `sha256:${createHash('sha256').update(stableStringify(value)).digest('hex')}`
}

/** First 32 hex chars of the digest (the `graph_*` id suffixes use exactly this). */
export function stableHash32(value: unknown): string {
  const hash = stableHash(value)
  return hash.slice('sha256:'.length, 'sha256:'.length + 32)
}

function canonicalize(value: unknown, parentKey?: string): unknown {
  if (value === null) return null
  if (
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean'
  )
    return value
  if (typeof value === 'bigint') return value.toString()
  if (
    typeof value === 'undefined' ||
    typeof value === 'function' ||
    typeof value === 'symbol'
  ) {
    return `[${typeof value}]`
  }
  if (Array.isArray(value)) {
    const items = value.map(item => canonicalize(item))
    if (!shouldSortArray(parentKey)) return items
    return items.slice().sort((a, b) => stableStringify(a).localeCompare(stableStringify(b)))
  }
  if (value instanceof Date) return value.toISOString()
  if (!isRecord(value)) return Object.prototype.toString.call(value)
  const out: Record<string, unknown> = {}
  for (const key of Object.keys(value).sort()) {
    out[key] = canonicalize(value[key], key)
  }
  return out
}

function shouldSortArray(parentKey: string | undefined): boolean {
  return parentKey === 'required' || parentKey === 'enum'
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}
