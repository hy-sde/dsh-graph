/**
 * Schedule reconciliation: the authored algorithm that turns durable schedule
 * rows into operator provisions, exactly-once intent claims, and executions
 * (Maka `stream-graph-schedule-reconcile`, port subset). The loop never
 * trusts memory: every attempt re-reads the store snapshot; all mutation
 * linearizes against the schedule revision (CAS), and a revision conflict is
 * treated as `stale` → full re-snapshot → retry (≤8 attempts).
 * @module
 */

import {
  AgentGraphScheduleRevisionConflictError,
  type AgentGraphIntentClaimRecord,
  type AgentGraphIntentClaimResult,
  type AgentGraphOperatorProvision,
  type AgentGraphOperatorProvisionRequest,
  type AgentGraphScheduledWork,
  type AgentGraphScheduleUpdate,
  type GraphControlStore,
} from '@hy-sde-org/dsh-graph-control'
import { claimAgentGraphRunnableIntent } from './admission.ts'
import type { AgentGraphInputHandoff } from './handoff.ts'
import { stableHash, stableHash32 } from './hash.ts'
import { compareAgentGraphIdentity } from './identity.ts'
import type { AgentGraphRecordSource } from './projection.ts'
import { projectAgentGraphSchedule } from './schedule-projection.ts'
import { graphEdgeId } from './trace.ts'
import type {
  AgentGraphDeferredWorkKind,
  AgentGraphExecutor,
  AgentGraphOperatorBinding,
  AgentGraphRecord,
  AgentGraphRunnableIntent,
  AgentGraphReconciliationResult,
  AgentGraphScheduleProjection,
  AgentGraphSupervisorObservation,
  AgentGraphReconciliationTopology,
} from './types.ts'
export const MAX_RECONCILIATION_ATTEMPTS = 8
export const SCHEDULE_INTENT_SCHEMA_VERSION = 1 as const

/* ------------------------------- snapshot ----------------------------- */

export interface AgentGraphScheduleSnapshot {
  readonly graphId: string
  readonly topology: AgentGraphReconciliationTopology
  readonly observation: AgentGraphSupervisorObservation
  readonly schedule: AgentGraphScheduleProjection
  readonly updates: readonly AgentGraphScheduleUpdate[]
  readonly provisions: readonly AgentGraphOperatorProvision[]
  readonly claims: readonly AgentGraphIntentClaimRecord[]
  readonly selectedResultRecords: ReadonlyMap<string, AgentGraphRecord>
}

export interface AgentGraphReconcileSeams {
  readonly store: Pick<
    GraphControlStore,
    | 'listScheduleUpdates'
    | 'listOperatorProvisions'
    | 'listAgentGraphIntentClaims'
    | 'claimIntentAtScheduleRevision'
    | 'beginAgentGraphIntentExecutionAtScheduleRevision'
    | 'cancelAgentGraphIntentExecution'
  >
  readonly executor: AgentGraphExecutor
  readonly recordSource: AgentGraphRecordSource
  readonly observeGraph: (
    topology: AgentGraphReconciliationTopology,
  ) => Promise<AgentGraphSupervisorObservation>
  readonly newId: () => string
  readonly maxNewActivations: number
  readonly resolveSelectedResultInputs?: (
    inputs: readonly { sourceGraphId: string; resultId: string }[],
  ) => Promise<AgentGraphRecord[]>
  readonly hydrateInputHandoffs?: (
    records: readonly AgentGraphRecord[],
  ) => Promise<AgentGraphInputHandoff[]>
  readonly renderPrompt: (input: {
    work: AgentGraphScheduledWork
    inputRecords: readonly AgentGraphRecord[]
    inputHandoffs: readonly AgentGraphInputHandoff[]
  }) => string | Promise<string>
  readonly abortSignal?: AbortSignal
}

export interface AgentGraphScheduleStopResult {
  readonly targetId: string
  readonly reason: string
  readonly status:
  | 'stopped'
  | 'already_terminal'
  | 'cancelled_before_runtime'
  | 'ignored_unknown'
  readonly sessionId?: string
  readonly activationId?: string
}

export interface AgentGraphScheduleDeferredWork {
  readonly workId: string
  readonly reason: AgentGraphDeferredWorkKind
  readonly missingInputIds?: readonly string[]
}

export interface AgentGraphScheduleReconciliationResult extends AgentGraphReconciliationResult {
  readonly schedule: AgentGraphScheduleProjection
  readonly newActivationCount: number
  readonly observedExistingActivationCount: number
  readonly dispatches: readonly {
    intentId: string
    workId: string
    claimCreated: boolean
  }[]
  readonly stops: readonly AgentGraphScheduleStopResult[]
  readonly deferredWork: readonly AgentGraphScheduleDeferredWork[]
  readonly failures: readonly {
    phase: string
    workId?: string
    error: unknown
  }[]
}

/* ------------------------------ id helpers ---------------------------- */

export function scheduledWorkIntentId(graphId: string, workId: string): string {
  return `graph_intent_${stableHash32({ schemaVersion: SCHEDULE_INTENT_SCHEMA_VERSION, graphId, workId })}`
}

export function dynamicOperatorId(graphId: string, workId: string): string {
  return `graph_operator_${stableHash32({ schemaVersion: 1, kind: 'dynamic_operator', graphId, workId })}`
}

export function provisionFingerprint(
  graphId: string,
  work: AgentGraphScheduledWork,
  operatorId: string,
  edges: readonly {
    edgeId: string
    fromOperatorId: string
    toOperatorId: string
  }[],
): string {
  return stableHash({
    schemaVersion: 1,
    kind: 'provision',
    graphId,
    workId: work.workId,
    target: work.target,
    operatorId,
    edges: [...edges].sort((a, b) =>
      compareAgentGraphIdentity(a.edgeId, b.edgeId),
    ),
  })
}

/* --------------------------- work → intent ---------------------------- */

export interface ScheduledWorkIntentInput {
  readonly graphId: string
  readonly observation: AgentGraphSupervisorObservation
  readonly topology: AgentGraphReconciliationTopology
  readonly work: AgentGraphScheduledWork
  readonly provision?: AgentGraphOperatorProvision
}

/**
 * Builds the deterministic supervisor intent for one requested work item.
 * Throws when the work's dispatch shape conflicts with its provision status
 * (identity changed mid-reconciliation).
 */
export function scheduledWorkIntent(
  input: ScheduledWorkIntentInput,
): AgentGraphRunnableIntent {
  const { work, graphId } = input
  const operatorTarget = work.target.kind === 'operator'
  if (operatorTarget) {
    if (input.provision !== undefined) {
      throw new Error(
        `agent graph ${graphId}: operator-targeted work ${work.workId} must not have a provision`,
      )
    }
  } else if (input.provision === undefined) {
    throw new Error(
      `agent graph ${graphId}: work ${work.workId} requires a provision`,
    )
  }
  const operatorId = operatorTarget
    ? work.target.id
    : (input.provision?.operatorId as string)
  const binding = input.topology.operators.find(
    operator => operator.operatorId === operatorId,
  )
  const observationBinding = input.observation.operators.find(
    operator => operator.operatorId === operatorId,
  )
  if (
    binding === undefined ||
    observationBinding === undefined ||
    binding.sessionId !== observationBinding.sessionId
  ) {
    throw new Error(
      `agent graph ${graphId}: work ${work.workId} operator ${operatorId} identity changed mid-reconciliation`,
    )
  }
  const selectedResultInputs = work.selectedResultInputs ?? []
  const policyFingerprint = stableHash({
    schemaVersion: SCHEDULE_INTENT_SCHEMA_VERSION,
    kind: 'supervisor',
    graphId,
    workId: work.workId,
    target: work.target,
    inputIds: work.inputIds,
    ...(selectedResultInputs.length > 0 ? { selectedResultInputs } : {}),
    ...(work.replaces !== undefined ? { replaces: work.replaces } : {}),
  })
  const readinessContextFingerprint = stableHash({
    schemaVersion: SCHEDULE_INTENT_SCHEMA_VERSION,
    graphId,
    workId: work.workId,
    operatorId,
    targetSessionId: binding.sessionId,
    inputIds: work.inputIds,
    ...(selectedResultInputs.length > 0 ? { selectedResultInputs } : {}),
  })
  return {
    schemaVersion: SCHEDULE_INTENT_SCHEMA_VERSION,
    intentId: scheduledWorkIntentId(graphId, work.workId),
    graphId,
    readinessContextFingerprint,
    policyFingerprint,
    readinessId: work.workId,
    operatorId,
    targetSessionId: binding.sessionId,
    inputIds: work.inputIds,
    selectedResultInputs,
    policyKind: 'supervisor',
    triggerRouteIds: [],
    triggerRecordIds: [
      ...work.inputIds,
      ...selectedResultInputs.map(item => item.resultId),
    ],
  }
}

/* ---------------------------- provisioning ---------------------------- */

export interface BuildOperatorProvisionInput {
  readonly graphId: string
  readonly work: AgentGraphScheduledWork
  readonly source: AgentGraphScheduleUpdate['source']
  readonly expectedScheduleRevision: number
  /** Operators that own the work's input records (upstream edge endpoints). */
  readonly sourceOperatorIds: readonly string[]
}

/** Deterministic operator + edge ids for one work item (Maka formulas). */
export function buildOperatorProvisionRequest(
  input: BuildOperatorProvisionInput,
  newId: () => string,
): AgentGraphOperatorProvisionRequest {
  if (input.work.target.kind === 'operator') {
    throw new Error(
      `agent graph ${input.graphId}: operator-targeted work ${input.work.workId} must not be provisioned`,
    )
  }
  const operatorId = dynamicOperatorId(input.graphId, input.work.workId)
  const edges = input.sourceOperatorIds.map(fromOperatorId => ({
    edgeId: graphEdgeId(
      input.graphId,
      input.work.workId,
      fromOperatorId,
      operatorId,
    ),
    fromOperatorId,
    toOperatorId: operatorId,
  }))
  return {
    provisionId: `graph_provision_${stableHash32({ schemaVersion: 1, kind: 'provision', graphId: input.graphId, workId: input.work.workId })}`,
    graphId: input.graphId,
    workId: input.work.workId,
    operatorId,
    targetSessionId: `graph_session_${stableHash32({ schemaVersion: 1, kind: 'operator_session', graphId: input.graphId, workId: input.work.workId })}`,
    initialTurnId: newId(),
    initialRunId: newId(),
    provisionFingerprint: provisionFingerprint(
      input.graphId,
      input.work,
      operatorId,
      edges,
    ),
    edges,
    expectedScheduleRevision: input.expectedScheduleRevision,
  }
}

/* ------------------------------- snapshot ----------------------------- */

export async function readAgentGraphScheduleSnapshot(
  seams: AgentGraphReconcileSeams,
  graphId: string,
): Promise<AgentGraphScheduleSnapshot> {
  const [updates, provisions, claims] = await Promise.all([
    seams.store.listScheduleUpdates(graphId),
    seams.store.listOperatorProvisions(graphId),
    seams.store.listAgentGraphIntentClaims(graphId),
  ])
  const topology = composeProvisionedTopology(graphId, provisions)
  const observation = await seams.observeGraph(topology)
  if (observation.graphId !== graphId) {
    throw new Error(
      `agent graph ${graphId}: observation belongs to ${observation.graphId}`,
    )
  }
  const schedule = projectAgentGraphSchedule(graphId, updates)

  const requestedWork = schedule.work.filter(
    work => work.status === 'requested',
  )
  const selectedInputs = requestedWork.flatMap(
    work => work.selectedResultInputs ?? [],
  )
  const selectedResultRecords = new Map<string, AgentGraphRecord>()
  if (
    selectedInputs.length > 0 &&
    seams.resolveSelectedResultInputs !== undefined
  ) {
    const distinct = new Map<
      string,
      { sourceGraphId: string; resultId: string }
    >()
    for (const item of selectedInputs) {
      distinct.set(`${item.sourceGraphId}\u0000${item.resultId}`, item)
    }
    const resolved = await seams.resolveSelectedResultInputs([
      ...distinct.values(),
    ])
    const cache = new Map<string, AgentGraphRecord | undefined>()
    for (let index = 0; index < resolved.length; index += 1) {
      const record = resolved[index]
      const item = [...distinct.values()][index]
      if (item === undefined) continue
      const key = `${item.sourceGraphId}\u0000${item.resultId}`
      if (
        record !== undefined &&
        (record.graphId !== item.sourceGraphId ||
          record.recordId !== item.resultId)
      )
        continue
      cache.set(key, record)
    }
    for (const item of distinct.values()) {
      const key = `${item.sourceGraphId}\u0000${item.resultId}`
      const record = cache.get(key)
      if (record !== undefined) selectedResultRecords.set(key, record)
    }
  }

  return {
    graphId,
    topology,
    observation,
    schedule,
    updates,
    provisions,
    claims,
    selectedResultRecords,
  }
}

export function composeProvisionedTopology(
  graphId: string,
  provisions: readonly AgentGraphOperatorProvision[],
): AgentGraphReconciliationTopology {
  const operators: AgentGraphOperatorBinding[] = []
  const edges: {
    edgeId: string
    fromOperatorId: string
    toOperatorId: string
  }[] = []
  const seenOperators = new Set<string>()
  const seenEdges = new Set<string>()
  for (const provision of [...provisions].sort((a, b) =>
    compareAgentGraphIdentity(a.provisionId, b.provisionId),
  )) {
    if (provision.graphId !== graphId) {
      throw new Error(
        `agent graph ${graphId}: provision ${provision.provisionId} belongs to ${provision.graphId}`,
      )
    }
    if (seenOperators.has(provision.operatorId)) {
      throw new Error(
        `agent graph ${graphId}: operator ${provision.operatorId} provisioned more than once`,
      )
    }
    seenOperators.add(provision.operatorId)
    operators.push({
      operatorId: provision.operatorId,
      sessionId: provision.targetSessionId,
    })
    for (const edge of provision.edges) {
      if (seenEdges.has(edge.edgeId)) {
        throw new Error(
          `agent graph ${graphId}: edge ${edge.edgeId} reused across provisions`,
        )
      }
      seenEdges.add(edge.edgeId)
      edges.push(edge)
    }
  }
  return { graphId, operators, edges }
}

/* ------------------------------ stop wave ----------------------------- */

export async function applyScheduleStops(
  snapshot: AgentGraphScheduleSnapshot,
  seams: AgentGraphReconcileSeams,
): Promise<{
  stops: AgentGraphScheduleStopResult[]
  failures: { phase: string; targetId: string; error: unknown }[]
}> {
  const requests = new Map<string, string>()
  for (const stopped of snapshot.schedule.stoppedTargets) {
    requests.set(stopped.targetId, stopped.reason)
  }
  for (const work of snapshot.schedule.work) {
    if (work.replaces !== undefined && !requests.has(work.replaces)) {
      requests.set(work.replaces, `Superseded by graph work ${work.workId}`)
    }
  }
  const workById = new Map(
    snapshot.schedule.work.map(work => [work.workId, work]),
  )
  const claimsByIntent = new Map(
    snapshot.claims.map(claim => [claim.intentId, claim]),
  )
  const sortedTargets = [...requests.keys()].sort(compareAgentGraphIdentity)
  const stops: AgentGraphScheduleStopResult[] = []
  const failures: { phase: string; targetId: string; error: unknown }[] = []
  const pendingBySession = new Map<
    string,
    {
      targetId: string
      reason: string
      sessionId: string
      activationId?: string
    }[]
  >()

  for (const targetId of sortedTargets) {
    const reason = requests.get(targetId) as string
    const work = workById.get(targetId)
    const claim =
      work === undefined ? undefined : claimsByIntent.get(scheduledWorkIntentId(snapshot.graphId, work.workId))
    const activation = snapshot.observation.operators.find(
      operator => operator.operatorId === claim?.targetOperatorId,
    )
    if (activation !== undefined && activation.terminal) {
      stops.push({
        targetId,
        reason,
        status: 'already_terminal',
        sessionId: activation.sessionId,
        activationId: activation.operatorId,
      })
      continue
    }
    if (claim !== undefined) {
      try {
        const transition = await seams.store.cancelAgentGraphIntentExecution(
          snapshot.graphId,
          claim.intentId,
          reason,
        )
        if (
          transition.previousState !== 'executing' &&
          activation === undefined
        ) {
          stops.push({
            targetId,
            reason,
            status: 'cancelled_before_runtime',
            sessionId: claim.targetSessionId,
            activationId: claim.targetRunId,
          })
          continue
        }
      } catch (error) {
        failures.push({ phase: 'stop', targetId, error })
        continue
      }
      // Executing activation: fall through to the batched session stop.
    } else if (work !== undefined) {
      stops.push({ targetId, reason, status: 'cancelled_before_runtime' })
      continue
    }
    if (work === undefined && activation === undefined) {
      stops.push({ targetId, reason, status: 'ignored_unknown' })
      continue
    }
    const sessionId = activation?.sessionId ?? claim?.targetSessionId
    if (sessionId === undefined) {
      stops.push({ targetId, reason, status: 'ignored_unknown' })
      continue
    }
    const pending = pendingBySession.get(sessionId) ?? []
    pending.push({
      targetId,
      reason,
      sessionId,
      ...(activation !== undefined
        ? { activationId: activation.operatorId }
        : {}),
    })
    pendingBySession.set(sessionId, pending)
  }

  for (const [sessionId, targets] of pendingBySession) {
    try {
      await seams.executor.stopSession(sessionId, {
        source: 'graph_supervisor',
      })
      for (const target of targets) {
        stops.push({
          targetId: target.targetId,
          reason: target.reason,
          status: 'stopped',
          sessionId,
          ...(target.activationId !== undefined
            ? { activationId: target.activationId }
            : {}),
        })
      }
    } catch (error) {
      for (const target of targets)
        failures.push({ phase: 'stop', targetId: target.targetId, error })
    }
  }
  return { stops, failures }
}

/* ------------------------------ dispatch ------------------------------ */

export interface DispatchScheduledWorkInput {
  readonly graphId: string
  readonly intent: AgentGraphRunnableIntent
  readonly executionInput: { prompt: string }
  readonly expectedScheduleRevision: number
  readonly provision?: AgentGraphOperatorProvision
  readonly abortSignal?: AbortSignal
}

export type DispatchScheduledWorkOutcome =
  | { status: 'fulfilled'; claim: AgentGraphIntentClaimResult }
  | { status: 'rejected'; error: unknown; claim?: AgentGraphIntentClaimResult }
  | { status: 'stale' }

export async function dispatchScheduledWork(
  input: DispatchScheduledWorkInput,
  seams: AgentGraphReconcileSeams,
): Promise<DispatchScheduledWorkOutcome> {
  if (input.abortSignal?.aborted === true) {
    return {
      status: 'rejected',
      error: new Error('agent graph dispatch aborted'),
    }
  }
  try {
    const admission = await claimAgentGraphRunnableIntent({
      intent: input.intent,
      claimAgentGraphIntentAtScheduleRevision: (request, expectedRevision) =>
        seams.store.claimIntentAtScheduleRevision(request, expectedRevision),
      expectedScheduleRevision: input.expectedScheduleRevision,
      newId: seams.newId,
      ...(input.provision !== undefined
        ? {
          targetTurnId: input.provision.initialTurnId,
          targetRunId: input.provision.initialRunId,
        }
        : {}),
      executionInput: input.executionInput,
    })
    await seams.executor.runClaimedAgentGraphIntent({
      intent: input.intent,
      claim: admission.claim,
      prompt: input.executionInput.prompt,
      admitExecution: async () => {
        const transition =
          await seams.store.beginAgentGraphIntentExecutionAtScheduleRevision(
            input.graphId,
            input.intent.intentId,
            input.expectedScheduleRevision,
          )
        return transition.state === 'cancelled'
          ? ('cancelled' as const)
          : ('executing' as const)
      },
      ...(input.abortSignal !== undefined
        ? { abortSignal: input.abortSignal }
        : {}),
    })
    return { status: 'fulfilled', claim: admission }
  } catch (error) {
    if (error instanceof AgentGraphScheduleRevisionConflictError)
      return { status: 'stale' }
    return { status: 'rejected', error }
  }
}

/* ------------------------------ reconcile ----------------------------- */

export interface ReconcileAgentGraphScheduleInput extends AgentGraphReconcileSeams {
  readonly graphId: string
  readonly supervisor?: { onReconciliationFailure?(error: unknown): void }
}

export async function reconcileAgentGraphSchedule(
  input: ReconcileAgentGraphScheduleInput,
): Promise<AgentGraphScheduleReconciliationResult> {
  if (
    !Number.isSafeInteger(input.maxNewActivations) ||
    input.maxNewActivations < 0
  ) {
    throw new Error(
      'agent graph maxNewActivations must be a non-negative safe integer',
    )
  }
  let snapshot = await readAgentGraphScheduleSnapshot(input, input.graphId)
  const stops: AgentGraphScheduleStopResult[] = []
  const failures: { phase: string; workId?: string; error: unknown }[] = []
  const deferredWork: AgentGraphScheduleDeferredWork[] = []
  const dispatches: {
    intentId: string
    workId: string
    claimCreated: boolean
  }[] = []
  let newActivationCount = 0
  let observedExistingActivationCount = 0

  for (let attempt = 0; attempt < MAX_RECONCILIATION_ATTEMPTS; attempt += 1) {
    if (input.abortSignal?.aborted === true) {
      return buildResult(
        input.graphId,
        snapshot,
        'cancelled',
        stops,
        dispatches,
        deferredWork,
        failures,
        newActivationCount,
        observedExistingActivationCount,
      )
    }
    const stopWave = await applyScheduleStops(snapshot, input)
    stops.push(...stopWave.stops)
    failures.push(...stopWave.failures)
    if (stopWave.failures.length > 0) {
      return buildResult(
        input.graphId,
        snapshot,
        'failed',
        stops,
        dispatches,
        deferredWork,
        failures,
        newActivationCount,
        observedExistingActivationCount,
      )
    }

    // (A) provisions — only work that needs a dynamic operator and has none yet.
    const provisionsByWork = new Map<string, AgentGraphOperatorProvision>()
    for (const provision of snapshot.provisions)
      provisionsByWork.set(provision.workId, provision)
    const workByWorkId = new Map<string, AgentGraphScheduledWork>()
    for (const work of snapshot.schedule.work)
      workByWorkId.set(work.workId, work)

    let topologyStale = false
    let topologyChanged = false
    for (const work of orderedRequestedWork(snapshot.schedule)) {
      if (snapshot.schedule.closed) {
        deferredWork.push({ workId: work.workId, reason: 'graph_closed' })
        continue
      }
      if (work.target.kind === 'operator') continue
      if (provisionsByWork.has(work.workId)) continue
      const missing = missingWorkInputIds(work, snapshot)
      if (missing.length > 0) {
        deferredWork.push({
          workId: work.workId,
          reason: 'input_not_committed',
          missingInputIds: missing,
        })
        continue
      }
      const request = buildOperatorProvisionRequest(
        {
          graphId: input.graphId,
          work: work,
          source: workSourceOf(work, snapshot.updates),
          expectedScheduleRevision: snapshot.schedule.revision,
          sourceOperatorIds: sourceOperatorIdsFor(work, snapshot),
        },
        input.newId,
      )
      try {
        const result = await input.executor.provisionOperator(request)
        if (result === undefined) {
          deferredWork.push({ workId: work.workId, reason: 'operator_provision_unavailable' })
          continue
        }
        provisionsByWork.set(work.workId, result.provision)
        topologyChanged = true
      } catch (error) {
        if (error instanceof AgentGraphScheduleRevisionConflictError) {
          topologyStale = true
          break
        }
        failures.push({ phase: 'topology', workId: work.workId, error })
      }
    }
    if (topologyChanged || topologyStale) {
      snapshot = await readAgentGraphScheduleSnapshot(input, input.graphId)
      if (failures.length === 0 && !topologyStale) continue
      if (failures.length > 0) {
        return buildResult(
          input.graphId,
          snapshot,
          'failed',
          stops,
          dispatches,
          deferredWork,
          failures,
          newActivationCount,
          observedExistingActivationCount,
        )
      }
      // topologyStale: fresh snapshot, retry the loop without returning.
      continue
    }

    // (B) supervisor intents for every requested work.
    const claimsByIntent = new Map<string, AgentGraphIntentClaimRecord>()
    for (const claim of snapshot.claims)
      claimsByIntent.set(claim.intentId, claim)
    const processedIntentIds = new Set<string>()
    const candidates: {
      work: AgentGraphScheduledWork
      intent: AgentGraphRunnableIntent
      existing: boolean
    }[] = []
    for (const work of orderedRequestedWork(snapshot.schedule)) {
      // Topologically pending: operator-targeted work runs on an existing
      // operator; everything else needs a provision (input_not_committed was
      // already recorded for it in phase A).
      if (work.target.kind !== 'operator' && !provisionsByWork.has(work.workId))
        continue
      let intent: AgentGraphRunnableIntent
      const provisionForWork = provisionsByWork.get(work.workId)
      try {
        intent = scheduledWorkIntent({
          graphId: input.graphId,
          observation: snapshot.observation,
          topology: snapshot.topology,
          work,
          ...(provisionForWork !== undefined
            ? { provision: provisionForWork }
            : {}),
        })
      } catch (error) {
        failures.push({ phase: 'schedule', workId: work.workId, error })
        continue
      }
      if (processedIntentIds.has(intent.intentId)) continue
      const existing = claimsByIntent.has(intent.intentId)
      if (snapshot.schedule.closed && !existing) {
        deferredWork.push({ workId: work.workId, reason: 'graph_closed' })
        continue
      }
      const missing = missingWorkInputIds(work, snapshot)
      if (missing.length > 0) {
        deferredWork.push({
          workId: work.workId,
          reason: 'input_not_committed',
          missingInputIds: missing,
        })
        continue
      }
      candidates.push({ work, intent, existing })
    }
    if (failures.length > 0) {
      return buildResult(
        input.graphId,
        snapshot,
        'failed',
        stops,
        dispatches,
        deferredWork,
        failures,
        newActivationCount,
        observedExistingActivationCount,
      )
    }

    // (C) select — existing claims always selected; new activations capped.
    const selected: {
      work: AgentGraphScheduledWork
      intent: AgentGraphRunnableIntent
      existing: boolean
      provision?: AgentGraphOperatorProvision
    }[] = []
    let budget = input.maxNewActivations - newActivationCount
    for (const candidate of candidates) {
      const provisionForCandidate = provisionsByWork.get(candidate.work.workId)
      const provisioned = {
        ...candidate,
        ...(provisionForCandidate !== undefined
          ? { provision: provisionForCandidate }
          : {}),
      }
      if (candidate.existing) {
        selected.push(provisioned)
        continue
      }
      if (budget <= 0) {
        deferredWork.push({
          workId: candidate.work.workId,
          reason: 'activation_limit',
        })
        continue
      }
      budget -= 1
      selected.push(provisioned)
    }

    // (D) render — any failure fails the wave.
    if (selected.length > 0) {
      const rendered: {
        work: AgentGraphScheduledWork
        intent: AgentGraphRunnableIntent
        prompt: string
        provision?: AgentGraphOperatorProvision
      }[] = []
      for (const entry of selected) {
        try {
          const inputRecords = resolveInputRecords(entry.work, snapshot)
          const inputHandoffs =
            input.hydrateInputHandoffs === undefined
              ? []
              : await input.hydrateInputHandoffs(inputRecords)
          const prompt = await input.renderPrompt({
            work: entry.work,
            inputRecords,
            inputHandoffs,
          })
          if (prompt.trim().length === 0)
            throw new Error(
              `agent graph ${input.graphId}: rendered empty prompt for work ${entry.work.workId}`,
            )
          rendered.push({
            work: entry.work,
            intent: entry.intent,
            prompt,
            ...(entry.provision !== undefined
              ? { provision: entry.provision }
              : {}),
          })
        } catch (error) {
          failures.push({ phase: 'render', workId: entry.work.workId, error })
        }
      }
      if (failures.length > 0) {
        return buildResult(
          input.graphId,
          snapshot,
          'failed',
          stops,
          dispatches,
          deferredWork,
          failures,
          newActivationCount,
          observedExistingActivationCount,
        )
      }
      // (E) execute — all-or-per-outcome; revision conflicts are stale.
      for (const entry of rendered) {
        const outcome = await dispatchScheduledWork(
          {
            graphId: input.graphId,
            intent: entry.intent,
            executionInput: { prompt: entry.prompt },
            expectedScheduleRevision: snapshot.schedule.revision,
            ...(entry.provision !== undefined
              ? { provision: entry.provision }
              : {}),
            ...(input.abortSignal !== undefined
              ? { abortSignal: input.abortSignal }
              : {}),
          },
          input,
        )
        if (outcome.status === 'stale') break
        if (outcome.status === 'rejected') {
          failures.push({
            phase: 'dispatch',
            workId: entry.work.workId,
            error: outcome.error,
          })
          if (outcome.claim !== undefined) {
            processedIntentIds.add(entry.intent.intentId)
            if (outcome.claim.created) newActivationCount += 1
            else observedExistingActivationCount += 1
          }
          continue
        }
        dispatches.push({
          intentId: entry.intent.intentId,
          workId: entry.work.workId,
          claimCreated: outcome.claim.created,
        })
        processedIntentIds.add(entry.intent.intentId)
        if (outcome.claim.created) newActivationCount += 1
        else observedExistingActivationCount += 1
      }
    }

    const nextSnapshot = await readAgentGraphScheduleSnapshot(
      input,
      input.graphId,
    )
    if (nextSnapshot.schedule.revision !== snapshot.schedule.revision) {
      snapshot = nextSnapshot
      continue
    }
    snapshot = nextSnapshot
    const hasLimit = deferredWork.some(
      item => item.reason === 'activation_limit',
    )
    const hasWaiting = deferredWork.some(
      item =>
        item.reason === 'agent_topology_required' ||
        item.reason === 'input_not_committed',
    )
    const status = hasLimit
      ? 'limit_reached'
      : hasWaiting
        ? 'waiting'
        : 'reconciled'
    return buildResult(
      input.graphId,
      snapshot,
      status,
      stops,
      dispatches,
      deferredWork,
      failures,
      newActivationCount,
      observedExistingActivationCount,
    )
  }

  // Exhausted attempts.
  snapshot = await readAgentGraphScheduleSnapshot(input, input.graphId)
  return buildResult(
    input.graphId,
    snapshot,
    'stale',
    stops,
    dispatches,
    deferredWork,
    failures,
    newActivationCount,
    observedExistingActivationCount,
  )
}

/* ------------------------------- helpers ------------------------------ */

function orderedRequestedWork(
  schedule: AgentGraphScheduleProjection,
): AgentGraphScheduledWork[] {
  return schedule.work
    .filter(work => work.status === 'requested')
    .sort(
      (a, b) =>
        a.revision - b.revision ||
        a.committedAt - b.committedAt ||
        compareAgentGraphIdentity(a.workId, b.workId),
    )
}

function workSourceOf(
  work: AgentGraphScheduledWork,
  updates: readonly AgentGraphScheduleUpdate[],
): AgentGraphScheduleUpdate['source'] {
  for (const update of updates) {
    if (update.addWork.some(item => item.workId === work.workId))
      return update.source
  }
  throw new Error(`agent graph: work ${work.workId} has no source update`)
}

function sourceOperatorIdsFor(
  work: AgentGraphScheduledWork,
  snapshot: AgentGraphScheduleSnapshot,
): string[] {
  const byRecordId = new Map<string, AgentGraphRecord>()
  for (const record of snapshot.observation.records)
    byRecordId.set(record.recordId, record)
  const operatorIds = new Set<string>()
  for (const inputId of work.inputIds) {
    const record = byRecordId.get(inputId)
    if (record !== undefined) operatorIds.add(record.operatorId)
  }
  return [...operatorIds].sort(compareAgentGraphIdentity)
}

export function missingWorkInputIds(
  work: AgentGraphScheduledWork,
  snapshot: AgentGraphScheduleSnapshot,
): string[] {
  const committed = new Set(
    snapshot.observation.records.map(record => record.recordId),
  )
  const missing: string[] = []
  for (const inputId of work.inputIds) {
    if (!committed.has(inputId)) missing.push(inputId)
  }
  for (const selected of work.selectedResultInputs ?? []) {
    const key = `${selected.sourceGraphId}\u0000${selected.resultId}`
    if (!snapshot.selectedResultRecords.has(key))
      missing.push(selected.resultId)
  }
  return missing
}

function resolveInputRecords(
  work: AgentGraphScheduledWork,
  snapshot: AgentGraphScheduleSnapshot,
): AgentGraphRecord[] {
  const byRecordId = new Map<string, AgentGraphRecord>()
  for (const record of snapshot.observation.records)
    byRecordId.set(record.recordId, record)
  const result: AgentGraphRecord[] = []
  for (const inputId of work.inputIds) {
    const record = byRecordId.get(inputId)
    if (record !== undefined) result.push(record)
  }
  for (const selected of work.selectedResultInputs ?? []) {
    const record = snapshot.selectedResultRecords.get(
      `${selected.sourceGraphId}\u0000${selected.resultId}`,
    )
    if (record !== undefined) result.push(record)
  }
  return result
}

function buildResult(
  _graphId: string,
  snapshot: AgentGraphScheduleSnapshot,
  status: AgentGraphReconciliationResult['status'],
  stops: AgentGraphScheduleStopResult[],
  dispatches: { intentId: string; workId: string; claimCreated: boolean }[],
  deferredWork: AgentGraphScheduleDeferredWork[],
  failures: { phase: string; workId?: string; error: unknown }[],
  newActivationCount: number,
  observedExistingActivationCount: number,
): AgentGraphScheduleReconciliationResult {
  return {
    status,
    scheduledCandidateCount: dispatches.length,
    dispatched: dispatches.length,
    failures,
    deferred: deferredWork.map(item => ({
      workId: item.workId,
      reason: item.reason,
    })),
    schedule: snapshot.schedule,
    newActivationCount,
    observedExistingActivationCount,
    dispatches,
    stops,
    deferredWork,
  }
}
