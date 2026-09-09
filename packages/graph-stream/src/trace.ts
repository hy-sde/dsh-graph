/**
 * DAG validation + reference-only route projection over committed records
 * (Maka `stream-graph-trace`). Edges are added monotonically with operator
 * provisions and only ever point into the new operator, so the topology is a
 * DAG by construction — this module validates that invariant and derives one
 * route per (record × outgoing edge).
 * @module
 */

import { stableHash32 } from './hash.ts'
import { compareAgentGraphIdentity } from './identity.ts'
import type {
  AgentGraphRecord,
  AgentGraphTraceEdge,
  AgentGraphTraceRoute,
  AgentGraphTraceTopology,
} from './types.ts'

export function graphEdgeId(
  graphId: string,
  workId: string,
  fromOperatorId: string,
  toOperatorId: string,
): string {
  return `graph_edge_${stableHash32({
    schemaVersion: 1,
    kind: 'dynamic_edge',
    graphId,
    workId,
    fromOperatorId,
    toOperatorId,
  })}`
}

export function graphRouteId(
  graphId: string,
  edge: AgentGraphTraceEdge,
  sourceRecordId: string,
): string {
  return `graph_route_${stableHash32({
    schemaVersion: 1,
    graphId,
    edgeId: edge.edgeId,
    fromOperatorId: edge.fromOperatorId,
    toOperatorId: edge.toOperatorId,
    sourceRecordId,
  })}`
}

export interface AgentGraphTraceError extends Error {
  reason:
    | 'unknown_operator'
    | 'self_loop'
    | 'duplicate_endpoint'
    | 'duplicate_operator'
    | 'cycle'
}

function traceError(
  reason: AgentGraphTraceError['reason'],
  message: string,
): AgentGraphTraceError {
  const error = new Error(message) as AgentGraphTraceError
  error.reason = reason
  return error
}

/**
 * Validate a trace topology (operators from provisions, edges from provisions).
 * Rules (Maka order): unique operator ids, known endpoints, no self-loop, no
 * repeated ordered pair, acyclic (Kahn with identity tie-break).
 */
export function validateAgentGraphTraceTopology(
  graphId: string,
  operatorIds: readonly string[],
  edges: readonly AgentGraphTraceEdge[],
): AgentGraphTraceTopology {
  if (graphId.trim().length === 0)
    throw traceError('duplicate_operator', 'agent graph id must not be empty')
  const known = new Set<string>()
  for (const operatorId of operatorIds) {
    if (known.has(operatorId)) {
      throw traceError(
        'duplicate_operator',
        `agent graph ${graphId}: duplicate operator ${operatorId}`,
      )
    }
    known.add(operatorId)
  }

  const sortedEdges = [...edges].sort(
    (a, b) =>
      compareAgentGraphIdentity(a.fromOperatorId, b.fromOperatorId) ||
      compareAgentGraphIdentity(a.toOperatorId, b.toOperatorId) ||
      compareAgentGraphIdentity(a.edgeId, b.edgeId),
  )
  const seenEdgeIds = new Set<string>()
  const seenEndpoints = new Set<string>()
  for (const edge of sortedEdges) {
    if (edge.edgeId.trim().length === 0)
      throw traceError(
        'duplicate_endpoint',
        `agent graph ${graphId}: empty edge id`,
      )
    if (seenEdgeIds.has(edge.edgeId))
      throw traceError(
        'duplicate_endpoint',
        `agent graph ${graphId}: duplicate edge id ${edge.edgeId}`,
      )
    seenEdgeIds.add(edge.edgeId)
    if (!known.has(edge.fromOperatorId)) {
      throw traceError(
        'unknown_operator',
        `agent graph ${graphId}: edge ${edge.edgeId} references unknown operator ${edge.fromOperatorId}`,
      )
    }
    if (!known.has(edge.toOperatorId)) {
      throw traceError(
        'unknown_operator',
        `agent graph ${graphId}: edge ${edge.edgeId} references unknown operator ${edge.toOperatorId}`,
      )
    }
    if (edge.fromOperatorId === edge.toOperatorId) {
      throw traceError(
        'self_loop',
        `agent graph ${graphId}: edge ${edge.edgeId} cannot be a self-loop on ${edge.fromOperatorId}`,
      )
    }
    const endpointKey = `${edge.fromOperatorId}\u0000${edge.toOperatorId}`
    if (seenEndpoints.has(endpointKey)) {
      throw traceError(
        'duplicate_endpoint',
        `agent graph ${graphId}: multiple edges from ${edge.fromOperatorId} to ${edge.toOperatorId}`,
      )
    }
    seenEndpoints.add(endpointKey)
  }

  // Cycle check: Kahn's algorithm with identity tie-break (deterministic).
  const incoming = new Map<string, string[]>()
  const outgoing = new Map<string, string[]>()
  for (const operator of operatorIds) {
    incoming.set(operator, [])
    outgoing.set(operator, [])
  }
  for (const edge of sortedEdges) {
    outgoing.set(edge.fromOperatorId, [
      ...(outgoing.get(edge.fromOperatorId) ?? []),
      edge.toOperatorId,
    ])
    incoming.set(edge.toOperatorId, [
      ...(incoming.get(edge.toOperatorId) ?? []),
      edge.fromOperatorId,
    ])
  }
  const ready = [...operatorIds]
    .filter(operator => (incoming.get(operator) ?? []).length === 0)
    .sort(compareAgentGraphIdentity)
  const ordered: string[] = []
  while (ready.length > 0) {
    const current = ready.shift() as string
    ordered.push(current)
    const nextOperators = [...(outgoing.get(current) ?? [])].sort(
      compareAgentGraphIdentity,
    )
    for (const next of nextOperators) {
      const remaining = (incoming.get(next) ?? []).filter(
        operator => operator !== current,
      )
      incoming.set(next, remaining)
      if (remaining.length === 0) insertSorted(ready, next)
    }
  }
  if (ordered.length !== operatorIds.length) {
    const remaining = operatorIds
      .filter(operator => !ordered.includes(operator))
      .sort(compareAgentGraphIdentity)
    throw traceError(
      'cycle',
      `agent graph ${graphId}: trace graph contains a cycle involving: ${remaining.join(', ')}`,
    )
  }
  return { graphId, operators: [...operatorIds], edges: sortedEdges }
}

function insertSorted(sorted: string[], value: string): void {
  for (let index = 0; index < sorted.length; index += 1) {
    const existing = sorted[index]
    if (existing === undefined) break
    if (compareAgentGraphIdentity(value, existing) < 0) {
      sorted.splice(index, 0, value)
      return
    }
  }
  sorted.push(value)
}

/**
 * Derive a reference-only route per (committed record × outgoing edge of the
 * record's operator). Edges own visibility; readiness policies decide what a
 * record triggers.
 */
export function buildAgentGraphTraceSnapshot(
  graphId: string,
  operatorIds: readonly string[],
  edges: readonly AgentGraphTraceEdge[],
  emitted: ReadonlyMap<string, readonly AgentGraphRecord[]>,
): { topology: AgentGraphTraceTopology; routes: AgentGraphTraceRoute[] } {
  const topology = validateAgentGraphTraceTopology(graphId, operatorIds, edges)
  const routes: AgentGraphTraceRoute[] = []
  for (const edge of topology.edges) {
    for (const record of emitted.get(edge.fromOperatorId) ?? []) {
      const edgeRef = {
        edgeId: edge.edgeId,
        fromOperatorId: edge.fromOperatorId,
        toOperatorId: edge.toOperatorId,
      }
      routes.push({
        routeId: graphRouteId(graphId, edgeRef, record.recordId),
        edgeId: edge.edgeId,
        sourceOperatorId: edge.fromOperatorId,
        targetOperatorId: edge.toOperatorId,
        sourceRecordId: record.recordId,
        sourceActivationId: `${record.source.sessionId}:${record.source.runId}`,
      })
    }
  }
  routes.sort((a, b) => compareAgentGraphIdentity(a.routeId, b.routeId))
  return { topology, routes }
}
