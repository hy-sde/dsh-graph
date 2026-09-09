/**
 * Durable control plane for the Agent Graph (Maka port, slice P1).
 *
 * This package owns the *decisions*, not the derivation. Following Maka's
 * split (verified against `docs/architecture/agent-graph-stream-scheduling-draft.md`):
 * schedule updates, intent claims, operator provisions, and supervisor wakes are
 * the only stateful rows; records, routes, readiness intents, work status, and
 * client snapshots are derived elsewhere (session-projection folds).
 *
 * Storage shape: one {@link KvUnit} (`name: 'agent_graph'`) with a table per
 * record kind, plus derived uniqueness indexes that are rebuilt on open (see
 * {@link GraphControlStore}).
 * @module @hy-sde-org/dsh-graph-control
 */

export {
  GraphControlStore,
  AGENT_GRAPH_CONTROL_UNIT_VERSION,
  AGENT_GRAPH_CONTROL_UNIT_NAME,
} from './store.ts'
export type { AgentGraphIntentClaimRecord } from './store.ts'
export {
  AgentGraphScheduleRevisionConflictError,
  AgentGraphScheduleClosedError,
  AgentGraphIntentClaimConflictError,
  GraphControlError,
} from './errors.ts'
export type {
  AgentGraphScheduleUpdateRequest,
  AgentGraphScheduleUpdate,
  AgentGraphScheduleUpdateResult,
  AgentGraphScheduledWork,
  AgentGraphWorkTarget,
  AgentGraphSelectedResultInput,
  AgentGraphScheduleStop,
  AgentGraphScheduleFinish,
  AgentGraphScheduleUpdateSource,
  AgentGraphIntentClaimRequest,
  AgentGraphIntentClaim,
  AgentGraphIntentClaimResult,
  AgentGraphIntentAdmissionState,
  AgentGraphIntentAdmissionTransition,
  AgentGraphOperatorProvisionRequest,
  AgentGraphOperatorProvision,
  AgentGraphOperatorBinding,
  AgentGraphProvisionedEdge,
  AgentGraphOperatorProvisionResult,
  AgentGraphSupervisorWakeRecord,
  AgentGraphSupervisorWakeAttemptRecord,
  AgentGraphSupervisorWakeStatus,
  ClaimAgentGraphSupervisorWakeRequest,
  BeginAgentGraphSupervisorWakeAttemptRequest,
  CompleteAgentGraphSupervisorWakeAttemptRequest,
  SupersedeAgentGraphSupervisorWakesRequest,
  AgentGraphControlSnapshot,
} from './types.ts'
export {
  AGENT_GRAPH_SCHEDULE_SCHEMA_VERSION,
  AGENT_GRAPH_INTENT_CLAIM_SCHEMA_VERSION,
  MAX_ADD_WORK,
  MAX_INPUT_IDS,
  MAX_SELECTED_RESULT_INPUTS,
  MAX_SCHEDULE_INSTRUCTION_LENGTH,
  MAX_WORK_STOPS,
  graphUpdateId,
  graphIntentId,
  graphOperatorId,
  graphClaimId,
  graphProvisionId,
  graphWakeId,
  graphWakeAttemptId,
} from './types.ts'
