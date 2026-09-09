/**
 * Locale-independent total order for Agent Graph opaque ids (Maka
 * `stream-graph-identity`). `localeCompare` is banned: Unicode tie-breaking
 * depends on host locale; the graph derives deterministic orderings from
 * UTF-16 code-unit comparison everywhere.
 * @module
 */

/** -1 / 0 / 1 in the graph's canonical id order (`a < b ? -1 : a > b ? 1 : 0`). */
export function compareAgentGraphIdentity(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0
}
