/**
 * Stream-layer contracts for the Agent Graph (Maka port, slice P2).
 *
 * The stream layer is the *derivation* half of the graph: it turns durable
 * facts (schedule rows from `dsh-graph-control`, committed operator records
 * from a record source) into graph-derived views, and drives exactly-once
 * execution through the {@link AgentGraphExecutor} seam. Nothing here is
 * stateful — rows live in `graph-control`; records live at the record source
 * (P3 adapter over child sessions); this layer recomputes.
 * @module
 */

import type {
  AgentGraphIntentClaim,
  AgentGraphOperatorProvisionRequest,
  AgentGraphOperatorProvisionResult,
  AgentGraphScheduledWork,
  AgentGraphScheduleUpdate,
} from '@hy-sde-org/dsh-graph-control'
/* ------------------------------- records ------------------------------ */

/** One committed operator record: a *copy with provenance* (DSH has no immutable cross-session event log). */
export interface AgentGraphRecord {
  readonly recordId: string
  readonly graphId: string
  readonly operatorId: string
  readonly source: {
    readonly sessionId: string
    readonly runId: string
    readonly turnId?: string
    /** The durable runtime event id this record copies (adapter-provided provenance). */
    readonly runtimeEventId?: string
    /** Monotonic per-session ordering key from the source. */
    readonly seq: number
  }
  /** Bounded summary (final non-partial output text, truncated at 16 KiB at render time). */
  readonly summary: string
  readonly facets: readonly AgentGraphRecordFacet[]
  /** Reference-only links to further evidence (e.g. artifact refs); never payloads. */
  readonly evidenceRefs?: readonly string[]
  readonly emittedAt: number
}

export type AgentGraphRecordFacet = 'message' | 'terminal' | 'runtime_fact'
export interface AgentGraphActivationState {
  readonly operatorId: string
  readonly sessionId: string
  readonly runId: string
}

/** One recorded signal attached to an operator's latest state. */
export interface AgentGraphSupervisorSignal {
  readonly kind: 'attention' | 'terminal'
  readonly message: string
}

/* ----------------------------- topology ------------------------------- */

/** One operator binding: a durable child session owned by the graph. */
export interface AgentGraphOperatorBinding {
  readonly operatorId: string
  readonly sessionId: string
}

export interface AgentGraphReconciliationTopology {
  readonly graphId: string
  readonly operators: readonly AgentGraphOperatorBinding[]
  readonly edges: readonly {
    edgeId: string
    fromOperatorId: string
    toOperatorId: string
  }[]
}

/** The observation seam every host provides: committed records + operator terminal state. */
export interface AgentGraphSupervisorObservation {
  readonly graphId: string
  readonly records: readonly AgentGraphRecord[]
  readonly operators: readonly (AgentGraphOperatorBinding & {
    terminal: boolean
  })[]
}

/* -------------------------------- trace ------------------------------- */

export interface AgentGraphTraceEdge {
  readonly edgeId: string
  readonly fromOperatorId: string
  readonly toOperatorId: string
}

export interface AgentGraphTraceRoute {
  readonly routeId: string
  readonly edgeId: string
  readonly sourceOperatorId: string
  readonly targetOperatorId: string
  readonly sourceRecordId: string
  readonly sourceActivationId: string
}

export interface AgentGraphTraceTopology {
  readonly graphId: string
  readonly operators: readonly string[]
  readonly edges: readonly AgentGraphTraceEdge[]
}

/* ------------------------------ readiness ----------------------------- */

export type AgentGraphReadinessPolicyKind = 'supervisor' | 'map'
export interface AgentGraphRunnableIntent {
  readonly schemaVersion: 1
  readonly intentId: string
  readonly graphId: string
  readonly readinessContextFingerprint: string
  readonly policyFingerprint: string
  readonly readinessId: string
  readonly operatorId: string
  readonly targetSessionId: string
  readonly inputIds: readonly string[]
  readonly selectedResultInputs: readonly {
    sourceGraphId: string
    resultId: string
  }[]
  readonly policyKind: AgentGraphReadinessPolicyKind
  readonly triggerRouteIds: readonly string[]
  readonly triggerRecordIds: readonly string[]
}

/** Readiness state derivation is owned by `readiness.ts` (map policy); see that module. */

/** Reasons a ready work item cannot run yet (deferred, not failed). */
export type AgentGraphDeferredWorkKind =
  | 'agent_topology_required'
  | 'input_not_committed'
  | 'graph_closed'
  | 'activation_limit'
  | 'operator_provision_unavailable'
export type AgentGraphReconcileStatus =
  'reconciled' | 'waiting' | 'limit_reached' | 'failed' | 'cancelled' | 'stale'
export interface AgentGraphReconciliationFailure {
  readonly phase: string
  readonly workId?: string
  readonly error: unknown
}

export interface AgentGraphReconciliationResult {
  readonly status: AgentGraphReconcileStatus
  readonly scheduledCandidateCount: number
  readonly dispatched: number
  readonly failures: readonly AgentGraphReconciliationFailure[]
  readonly deferred: readonly {
    workId: string
    reason: AgentGraphDeferredWorkKind
  }[]
}

/* ------------------------------ executor ------------------------------ */

/** The seam every host provides (Maka `AgentGraphCoordinatorRuntime`): the whole runtime surface the driver needs. */
export interface AgentGraphExecutor {
  /**
   * Provision one operator child (idempotent; adopts on retry). Returns the
   * durable provision row, or undefined when the host cannot provide the
   * operator yet (work is deferred, not failed).
   */
  provisionOperator(
    request: AgentGraphOperatorProvisionRequest,
  ): Promise<AgentGraphOperatorProvisionResult | undefined>
  /** Run one CLAIMED intent to completion and return its committed records. */
  runClaimedAgentGraphIntent(
    input: AgentGraphRunClaimedIntentInput,
  ): Promise<AgentGraphRecord[]>
  /** Stop a running operator child (graph-supervisor stops; batched per session by the driver). */
  stopSession(
    sessionId: string,
    opts?: { source?: 'graph_supervisor' },
  ): Promise<void>
}

export interface AgentGraphRunClaimedIntentInput {
  readonly intent: AgentGraphRunnableIntent
  readonly claim: AgentGraphIntentClaim
  readonly prompt: string
  /** Post-serialization admission gate: re-linearizes against the schedule revision. */
  readonly admitExecution?: () => Promise<'executing' | 'cancelled'>
  readonly abortSignal?: AbortSignal
}

/* ------------------------------ schedule ------------------------------ */

/** Pure work-status projection over the schedule update log (Maka `projectAgentGraphSchedule`). */
export type AgentGraphWorkStatus = 'requested' | 'stopped' | 'superseded'
export interface AgentGraphScheduleWorkView extends AgentGraphScheduledWork {
  status: AgentGraphWorkStatus
  readonly updateId: string
  readonly revision: number
  readonly committedAt: number
}

export interface AgentGraphScheduleProjection {
  readonly schemaVersion: 1
  readonly graphId: string
  readonly closed: boolean
  readonly revision: number
  readonly updateCount: number
  readonly work: readonly AgentGraphScheduleWorkView[]
  readonly stoppedTargets: readonly {
    targetId: string
    reason: string
    updateId: string
    revision: number
    committedAt: number
  }[]
  readonly finish?: {
    resultIds: readonly string[]
    reason: string
    updateId: string
    revision: number
    committedAt: number
  }
}

/* ---------------------------- handoff (render) ------------------------ */

/* Handoff types are owned by `handoff.ts` (bounded conclusion text + prompt render). */

/* ----------------------------- coordinator ---------------------------- */

export interface AgentGraphScheduleSource {
  readonly sessionId: string
  readonly runId: string
  readonly turnId: string
  readonly toolCallId: string
}

export interface AgentGraphScheduleInput {
  readonly graphId: string
  readonly source: AgentGraphScheduleSource
  readonly addWork: readonly AgentGraphScheduledWork[]
  readonly stop: readonly { targetId: string; reason: string }[]
  readonly finish?: { resultIds: readonly string[]; reason: string }
}

/**
 * Fire-and-forget observer contract: callbacks never fail an activation and
 * are never awaited (Maka `AgentGraphSupervisorObserver`).
 */
export interface AgentGraphSupervisorObserver {
  onObservation?(observation: AgentGraphSupervisorObservation): void
  onRuntimeEvent?(): void
  onReconciliationFailure?(error: unknown): void
}

export type { AgentGraphScheduleUpdate }
