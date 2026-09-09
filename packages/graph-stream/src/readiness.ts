/**
 * Readiness: derives deterministic runnable intents from committed records
 * and declared policies (Maka `stream-graph-readiness`, map policy only for
 * this slice — `all_settled` and supervisor-readiness kinds are cut until the
 * P4 supervisor tools need them). Derivation is deterministic: recomputing
 * this projection alone never starts work; admission is the store.
 * @module
 */

import { stableHash, stableHash32 } from './hash.ts'
import { compareAgentGraphIdentity } from './identity.ts'
import { buildAgentGraphTraceSnapshot } from './trace.ts'
import type {
  AgentGraphRecord,
  AgentGraphRunnableIntent,
  AgentGraphTraceEdge,
} from './types.ts'

export const AGENT_GRAPH_READINESS_SCHEMA_VERSION = 1 as const

export interface AgentGraphReadinessPolicy {
  readonly readinessId: string
  readonly operatorId: string
  readonly kind: 'map'
}

export type AgentGraphReadinessWait =
  | { kind: 'input_route'; upstreamOperatorIds: string[] }
  | { kind: 'activation_missing'; operatorId: string; activationId: string }
  | { kind: 'activation_running'; operatorId: string; activationId: string }

export interface AgentGraphOperatorReadinessState {
  readonly readinessId: string
  readonly operatorId: string
  readonly policyKind: 'map'
  readonly status: 'waiting' | 'runnable'
  readonly waitingFor?: readonly AgentGraphReadinessWait[]
  readonly intents: readonly AgentGraphRunnableIntent[]
}

export interface AgentGraphReadinessSnapshot {
  readonly intents: readonly AgentGraphRunnableIntent[]
  readonly operatorStates: readonly AgentGraphOperatorReadinessState[]
}

export interface BuildAgentGraphReadinessSnapshotInput {
  readonly graphId: string
  readonly operators: readonly { operatorId: string; sessionId: string }[]
  readonly edges: readonly AgentGraphTraceEdge[]
  readonly records: readonly AgentGraphRecord[]
  readonly policies: readonly AgentGraphReadinessPolicy[]
}

export interface AgentGraphReadinessSnapshotResult extends AgentGraphReadinessSnapshot {
  readonly routesCount: number
}

/**
 * Map policy: one intent per route received by the operator through a
 * declared incoming edge, sealed against exactly the records that triggered
 * it. Deterministic intentId / readiness fingerprints (Maka formulas).
 */
export function buildAgentGraphReadinessSnapshot(
  input: BuildAgentGraphReadinessSnapshotInput,
): AgentGraphReadinessSnapshotResult {
  if (input.graphId.trim().length === 0)
    throw new Error('agent graph id must not be empty')
  const byReadinessId = new Map<string, string>()
  const byOperator = new Map<string, string>()
  for (const policy of input.policies) {
    if (
      policy.readinessId.trim().length === 0 ||
      policy.operatorId.trim().length === 0
    ) {
      throw new Error(
        `agent graph ${input.graphId}: readiness policy with empty identity`,
      )
    }
    if (byReadinessId.has(policy.readinessId)) {
      throw new Error(
        `agent graph ${input.graphId}: duplicate readiness id ${policy.readinessId}`,
      )
    }
    if (byOperator.has(policy.operatorId)) {
      throw new Error(
        `agent graph ${input.graphId}: operator already has a readiness policy ${policy.operatorId}`,
      )
    }
    byReadinessId.set(policy.readinessId, policy.operatorId)
    byOperator.set(policy.operatorId, policy.readinessId)
  }
  const bindingById = new Map<string, string>()
  for (const operator of input.operators) {
    bindingById.set(operator.operatorId, operator.sessionId)
  }
  for (const policy of input.policies) {
    if (!bindingById.has(policy.operatorId)) {
      throw new Error(
        `agent graph ${input.graphId}: readiness policy ${policy.readinessId} targets unknown operator ${policy.operatorId}`,
      )
    }
  }

  const emitted = new Map<string, readonly AgentGraphRecord[]>()
  for (const record of input.records) {
    const list = emitted.get(record.operatorId) ?? []
    emitted.set(record.operatorId, [...list, record])
  }
  const { routes } = buildAgentGraphTraceSnapshot(
    input.graphId,
    input.operators.map(operator => operator.operatorId),
    input.edges,
    emitted,
  )
  const routesByTarget = new Map<string, typeof routes>()
  for (const route of routes) {
    const list = routesByTarget.get(route.targetOperatorId) ?? []
    routesByTarget.set(route.targetOperatorId, [...list, route])
  }

  const operatorSessions = new Map<string, string>()
  for (const operator of input.operators) {
    operatorSessions.set(operator.operatorId, operator.sessionId)
  }

  const intents: AgentGraphRunnableIntent[] = []
  const operatorStates: AgentGraphOperatorReadinessState[] = []

  for (const policy of [...input.policies].sort((a, b) =>
    compareAgentGraphIdentity(a.readinessId, b.readinessId),
  )) {
    const sessionId = operatorSessions.get(policy.operatorId) as string
    const policyFingerprint = stableHash({
      schemaVersion: AGENT_GRAPH_READINESS_SCHEMA_VERSION,
      ...policy,
    })
    const incomingEdges = input.edges
      .filter(edge => edge.toOperatorId === policy.operatorId)
      .map(edge => ({
        edgeId: edge.edgeId,
        fromOperatorId: edge.fromOperatorId,
        toOperatorId: edge.toOperatorId,
      }))
      .sort(
        (a, b) =>
          compareAgentGraphIdentity(a.fromOperatorId, b.fromOperatorId) ||
          compareAgentGraphIdentity(a.toOperatorId, b.toOperatorId) ||
          compareAgentGraphIdentity(a.edgeId, b.edgeId),
      )
    const readinessContextFingerprint = stableHash({
      schemaVersion: AGENT_GRAPH_READINESS_SCHEMA_VERSION,
      graphId: input.graphId,
      targetOperator: { operatorId: policy.operatorId, sessionId },
      incomingEdges,
      policyFingerprint,
    })

    const receivedRoutes = routesByTarget.get(policy.operatorId) ?? []
    const policyIntents: AgentGraphRunnableIntent[] = receivedRoutes.map(
      route =>
        runnableMapIntent(
          input,
          policy,
          policyFingerprint,
          readinessContextFingerprint,
          sessionId,
          [route],
        ),
    )
    intents.push(...policyIntents)
    let waitingFor: AgentGraphReadinessWait[] | undefined
    if (policyIntents.length === 0) {
      waitingFor = [
        {
          kind: 'input_route' as const,
          upstreamOperatorIds: incomingEdges.map(edge => edge.fromOperatorId).sort(compareAgentGraphIdentity),
        },
      ]
    }
    operatorStates.push({
      readinessId: policy.readinessId,
      operatorId: policy.operatorId,
      policyKind: 'map',
      status: policyIntents.length > 0 ? 'runnable' : 'waiting',
      intents: policyIntents,
      ...(waitingFor !== undefined ? { waitingFor } : {}),
    })
  }

  return { intents, operatorStates, routesCount: routes.length }
}

function runnableMapIntent(
  input: BuildAgentGraphReadinessSnapshotInput,
  policy: AgentGraphReadinessPolicy,
  policyFingerprint: string,
  readinessContextFingerprint: string,
  sessionId: string,
  routes: readonly { routeId: string; sourceRecordId: string }[],
): AgentGraphRunnableIntent {
  const triggerRouteIds = routes.map(route => route.routeId)
  const triggerRecordIds = routes.map(route => route.sourceRecordId)
  return {
    schemaVersion: AGENT_GRAPH_READINESS_SCHEMA_VERSION,
    intentId: `graph_intent_${stableHash32({
      schemaVersion: AGENT_GRAPH_READINESS_SCHEMA_VERSION,
      graphId: input.graphId,
      readinessContextFingerprint,
      policyFingerprint,
      readinessId: policy.readinessId,
      operatorId: policy.operatorId,
      targetSessionId: sessionId,
      policyKind: policy.kind,
      triggerRouteIds,
      triggerRecordIds,
    })}`,
    graphId: input.graphId,
    readinessContextFingerprint,
    policyFingerprint,
    readinessId: policy.readinessId,
    operatorId: policy.operatorId,
    targetSessionId: sessionId,
    inputIds: triggerRecordIds,
    selectedResultInputs: [],
    policyKind: 'map',
    triggerRouteIds,
    triggerRecordIds,
  }
}
