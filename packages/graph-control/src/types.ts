import { createHash } from 'node:crypto'

/**
 * Contract types for the Agent Graph control plane.
 *
 * Names, shapes, and invariants ported from Maka `packages/core/src/agent-graph-*
 * (schedule/control/topology/supervisor-wake)`; the fork-specific adaptations are
 * (a) no epoch table — one DSH session owns one graph, so graph identity is the
 * session identity for the first slice, and (b) no `schemaVersion` on every row
 * is enforced here beyond the constants below (the unit-level version stamp in
 * the storage layer covers it).
 * @module
 */

export const AGENT_GRAPH_SCHEDULE_SCHEMA_VERSION = 1 as const
export const AGENT_GRAPH_INTENT_CLAIM_SCHEMA_VERSION = 1 as const

/** Maka bounds, kept verbatim (see `agent-graph-schedule.ts`). */
export const MAX_ADD_WORK = 32
export const MAX_INPUT_IDS = 64
export const MAX_SELECTED_RESULT_INPUTS = 64
export const MAX_SCHEDULE_INSTRUCTION_LENGTH = 60_000
export const MAX_WORK_STOPS = 20

/** Identity of the runtime call that produced a supervisor decision. */
export interface AgentGraphScheduleUpdateSource {
  readonly sessionId: string
  readonly runId: string
  readonly turnId: string
  readonly toolCallId: string
  /** Maka allows the caller to stamp its orchestration mode; informational. */
  readonly orchestrationMode?: 'graph' | 'swarm'
}

/** Where a piece of work runs: a catalog agent, a preset, or an existing operator. */
export interface AgentGraphWorkTarget {
  readonly kind: 'agent' | 'preset' | 'operator'
  readonly id: string
}

/** A committed-result reference read from an earlier (possibly closed) graph epoch. */
export interface AgentGraphSelectedResultInput {
  readonly sourceGraphId: string
  readonly resultId: string
}

/** One piece of scheduled work on the graph. */
export interface AgentGraphScheduledWork {
  readonly workId: string
  /** Where this work runs: a catalog agent, an agent preset, or an existing operator. */
  readonly target: AgentGraphWorkTarget
  readonly instruction: string
  readonly inputIds: readonly string[]
  readonly selectedResultInputs?: readonly AgentGraphSelectedResultInput[]
  /** Work this item supersedes (the superseded work keeps its durable row). */
  readonly replaces?: string
  readonly replacementMode?: 'none' | 'replace'
}

/** Stop a work item or an activation. */
export interface AgentGraphScheduleStop {
  readonly targetId: string
  readonly reason: string
}

/** Terminal closure: the named committed record ids are the graph's selected results. */
export interface AgentGraphScheduleFinish {
  readonly resultIds: readonly string[]
  readonly reason: string
}

/** One idempotent supervisor decision (add work / stop / finish). */
export interface AgentGraphScheduleUpdateRequest {
  readonly schemaVersion: typeof AGENT_GRAPH_SCHEDULE_SCHEMA_VERSION
  /** Deterministic id: `graph_update_<sha256(graphId + source)>`; retries reuse it. */
  readonly updateId: string
  /** Deterministic fingerprint of the semantic payload; idempotency is verified against it. */
  readonly updateFingerprint: string
  readonly graphId: string
  readonly source: AgentGraphScheduleUpdateSource
  readonly addWork: readonly AgentGraphScheduledWork[]
  readonly stop: readonly AgentGraphScheduleStop[]
  readonly finish?: AgentGraphScheduleFinish
}

/** A committed schedule update: the request plus its durable revision/commit stamp. */
export interface AgentGraphScheduleUpdate extends AgentGraphScheduleUpdateRequest {
  readonly revision: number
  readonly committedAt: number
}

export interface AgentGraphScheduleUpdateResult {
  readonly update: AgentGraphScheduleUpdate
  readonly created: boolean
}

/**
 * Exactly-once admission authority for one runnable graph intent. The target
 * turn/run ids are allocated BEFORE any runtime action; a retry observes the
 * same activation identity instead of starting a second activation.
 */
export interface AgentGraphIntentClaimRequest {
  readonly schemaVersion: typeof AGENT_GRAPH_INTENT_CLAIM_SCHEMA_VERSION
  readonly claimId: string
  readonly graphId: string
  readonly intentId: string
  readonly intentFingerprint: string
  readonly readinessContextFingerprint: string
  readonly targetOperatorId: string
  readonly targetSessionId: string
  readonly targetTurnId: string
  readonly targetRunId: string
}

export interface AgentGraphIntentClaim extends AgentGraphIntentClaimRequest {
  readonly claimedAt: number
}

export interface AgentGraphIntentClaimResult {
  readonly claim: AgentGraphIntentClaim
  readonly created: boolean
}

export type AgentGraphIntentAdmissionState = 'claimed' | 'executing' | 'cancelled'

export interface AgentGraphIntentAdmissionTransition {
  readonly state: AgentGraphIntentAdmissionState
  readonly previousState: AgentGraphIntentAdmissionState
  readonly changed: boolean
}

/** One monotonic operator addition (DAG addition; edges only ever point into the new operator). */
export interface AgentGraphProvisionedEdge {
  readonly edgeId: string
  readonly fromOperatorId: string
  readonly toOperatorId: string
}

export interface AgentGraphOperatorProvisionRequest {
  readonly provisionId: string
  readonly graphId: string
  readonly workId: string
  /** Deterministic: `graph_operator_<sha256(graphId + workId)>`. */
  readonly operatorId: string
  readonly targetSessionId: string
  readonly initialTurnId: string
  readonly initialRunId: string
  /** Fingerprint over the full provision (target, edges, profile) — adopt-on-retry equality. */
  readonly provisionFingerprint: string
  readonly edges: readonly AgentGraphProvisionedEdge[]
  /** Expected schedule revision the provision is linearized against. */
  readonly expectedScheduleRevision: number
}

export interface AgentGraphOperatorProvision extends Omit<AgentGraphOperatorProvisionRequest, 'expectedScheduleRevision'> {
  readonly provisionedAt: number
}

export interface AgentGraphOperatorProvisionResult {
  readonly provision: AgentGraphOperatorProvision
  readonly created: boolean
}

/** One operator worktree binding: the durable lease a provisioned child runs in. */
export interface AgentGraphOperatorBinding {
  readonly graphId: string
  readonly workId: string
  readonly provisionId: string
  readonly leaseId: string
  /** Absolute workspace path of the leased worktree. */
  readonly path: string
  /** Absolute root of the repository the worktree belongs to. */
  readonly repoRoot: string
  readonly boundAt: number
}

export type AgentGraphSupervisorWakeStatus =
  | 'pending'
  | 'running'
  | 'waiting_permission'
  | 'delivered'
  | 'superseded'
  | 'retryable_failed'

export interface AgentGraphSupervisorWakeRecord {
  readonly wakeId: string
  readonly graphId: string
  readonly snapshotVersion: string
  readonly rootSessionId: string
  readonly status: AgentGraphSupervisorWakeStatus
  readonly attemptCount: number
  readonly currentAttemptId?: string
  readonly currentTurnId?: string
  readonly createdAt: number
  readonly updatedAt: number
  readonly supersededReason?: string
}

export interface AgentGraphSupervisorWakeAttemptRecord {
  readonly attemptId: string
  readonly wakeId: string
  readonly graphId: string
  readonly turnId: string
  readonly status: 'running' | 'waiting_permission' | 'delivered' | 'superseded' | 'retryable_failed'
  readonly startedAt: number
  readonly completedAt?: number
  readonly failureReason?: string
}

export interface ClaimAgentGraphSupervisorWakeRequest {
  readonly graphId: string
  readonly wakeId: string
  readonly snapshotVersion: string
  readonly rootSessionId: string
}

export interface BeginAgentGraphSupervisorWakeAttemptRequest {
  readonly graphId: string
  readonly wakeId: string
  readonly attemptId: string
  readonly turnId: string
}

export interface CompleteAgentGraphSupervisorWakeAttemptRequest {
  readonly graphId: string
  readonly wakeId: string
  readonly attemptId: string
  readonly status: 'waiting_permission' | 'delivered' | 'superseded' | 'retryable_failed'
  readonly failureReason?: string
}

export interface SupersedeAgentGraphSupervisorWakesRequest {
  readonly rootSessionIds: readonly string[]
  /** Optional exact graph identities; omitted means every graph under the roots. */
  readonly graphIds?: readonly string[]
  readonly reason: string
}

/** Whole-unit snapshot returned by {@link GraphControlStore.snapshot} (diagnostics/tests). */
export interface AgentGraphControlSnapshot {
  readonly scheduleUpdates: readonly AgentGraphScheduleUpdate[]
  readonly intentClaims: readonly AgentGraphIntentClaim[]
  readonly operatorProvisions: readonly AgentGraphOperatorProvision[]
  readonly operatorBindings: readonly AgentGraphOperatorBinding[]
  readonly supervisorWakes: readonly AgentGraphSupervisorWakeRecord[]
}

/* ------------------------------------------------------------------ */
/* Deterministic id helpers (sha256; Maka's `graph_*` id vocabulary).  */
/* ------------------------------------------------------------------ */


function hash(...parts: string[]): string {
  return createHash('sha256').update(parts.join('\u0000')).digest('hex')
}

export function graphUpdateId(graphId: string, source: AgentGraphScheduleUpdateSource): string {
  return `graph_update_${hash(graphId, source.sessionId, source.runId, source.turnId, source.toolCallId)}`
}

export function graphIntentId(graphId: string, workId: string): string {
  return `graph_intent_${hash(graphId, workId)}`
}

export function graphOperatorId(graphId: string, workId: string): string {
  return `graph_operator_${hash(graphId, workId)}`
}

export function graphClaimId(graphId: string, intentId: string): string {
  return `graph_claim_${hash(graphId, intentId)}`
}

export function graphProvisionId(graphId: string, workId: string): string {
  return `graph_provision_${hash(graphId, workId)}`
}

export function graphWakeId(graphId: string, snapshotVersion: string): string {
  return `graph_wake_${hash(graphId, snapshotVersion)}`
}

export function graphWakeAttemptId(wakeId: string, attemptIndex: number): string {
  return `graph_wake_attempt_${hash(wakeId, String(attemptIndex))}`
}
