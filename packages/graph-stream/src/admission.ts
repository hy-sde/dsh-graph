/**
 * Admission: binds a runnable intent to its resolved execution input BEFORE the
 * durable claim is written, then claims deterministically (Maka
 * `stream-graph-admission`). Binding before admission means a crash after the
 * claim but before runtime dispatch can never retry the same intent with
 * different work; the persisted claim remains authoritative for retries.
 * @module
 */

import { AGENT_GRAPH_INTENT_CLAIM_SCHEMA_VERSION } from '@hy-sde-org/dsh-graph-control'
import { stableHash } from './hash.ts'
import type {
  AgentGraphIntentClaimRequest,
  AgentGraphIntentClaimResult,
} from '@hy-sde-org/dsh-graph-control'
import type { AgentGraphRunnableIntent } from './types.ts'
export const AGENT_GRAPH_EXECUTION_INPUT_SCHEMA_VERSION = 1 as const

export interface AgentGraphRunnableIntentExecutionInput {
  readonly prompt: string
}

export interface FingerprintAgentGraphRunnableIntentInput {
  readonly intent: AgentGraphRunnableIntent
  readonly executionInput: AgentGraphRunnableIntentExecutionInput
}

export interface ClaimAgentGraphRunnableIntentInput {
  readonly intent: AgentGraphRunnableIntent
  /** Store seam: claim at an expected schedule revision (linearizes claim vs schedule). */
  readonly claimAgentGraphIntentAtScheduleRevision: (
    request: AgentGraphIntentClaimRequest,
    expectedScheduleRevision: number,
  ) => Promise<AgentGraphIntentClaimResult>
  readonly expectedScheduleRevision: number
  readonly newId: () => string
  /** Reuse topology-provisioned identities for a newly materialized operator (adopt-on-retry). */
  readonly targetTurnId?: string
  readonly targetRunId?: string
  readonly executionInput: AgentGraphRunnableIntentExecutionInput
}

/** Binds the complete runnable intent to its resolved execution input. */
export function fingerprintAgentGraphRunnableIntent(
  input: FingerprintAgentGraphRunnableIntentInput,
): string {
  if (input.executionInput.prompt.trim().length === 0) {
    throw new Error('Agent graph execution prompt must not be empty')
  }
  return stableHash({
    schemaVersion: AGENT_GRAPH_EXECUTION_INPUT_SCHEMA_VERSION,
    intent: input.intent,
    executionInput: input.executionInput,
  })
}

/**
 * Claims a deterministic readiness intent without invoking the runtime.
 * The store is the admission authority. Proposed turn/run ids are disposable
 * on an idempotent retry: the persisted identity always wins.
 */
export function claimAgentGraphRunnableIntent(
  input: ClaimAgentGraphRunnableIntentInput,
): Promise<AgentGraphIntentClaimResult> {
  const intentFingerprint = fingerprintAgentGraphRunnableIntent({
    intent: input.intent,
    executionInput: input.executionInput,
  })
  const claimHash = stableHash({
    schemaVersion: AGENT_GRAPH_INTENT_CLAIM_SCHEMA_VERSION,
    graphId: input.intent.graphId,
    intentId: input.intent.intentId,
  })
  return input.claimAgentGraphIntentAtScheduleRevision(
    {
      schemaVersion: AGENT_GRAPH_INTENT_CLAIM_SCHEMA_VERSION,
      claimId: `graph_claim_${claimHash.slice('sha256:'.length, 'sha256:'.length + 32)}`,
      graphId: input.intent.graphId,
      intentId: input.intent.intentId,
      intentFingerprint,
      readinessContextFingerprint: input.intent.readinessContextFingerprint,
      targetOperatorId: input.intent.operatorId,
      targetSessionId: input.intent.targetSessionId,
      targetTurnId: input.targetTurnId ?? input.newId(),
      targetRunId: input.targetRunId ?? input.newId(),
    },
    input.expectedScheduleRevision,
  )
}
