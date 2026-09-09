/**
 * The Agent Graph control store: single-writer, durable, heal-on-open.
 *
 * Persisted (authoritative) tables over one {@link KvUnit}:
 * - `schedule`          key = updateId                  → AgentGraphScheduleUpdate
 * - `claims`            key = `${graphId}:${intentId}`  → claim record (with admission status)
 * - `provisions`        key = provisionId               → AgentGraphOperatorProvision
 * - `operator_bindings` key = provisionId               → AgentGraphOperatorBinding
 * - `wakes`             key = wakeId                    → AgentGraphSupervisorWakeRecord
 * - `wake_attempts`     key = `${wakeId}:${attemptId}`  → AgentGraphSupervisorWakeAttemptRecord
 *
 * Derived indexes (schedule-by-revision, schedule-by-source, claim-target
 * uniqueness, provision-by-work, wake-by-root-and-graph) are rebuilt from the
 * authoritative rows at open — a torn multi-row write heals instead of
 * corrupting. The storage contract already forbids concurrent writers on one
 * unit ("the domain layer runs one write chain per unit"), so the in-process
 * promise-chain mutex below is the serialization point and per-record
 * `putRecord` calls supply durability.
 * @module
 */

import type { KvUnit } from '@deepseek-ai/dsh-storage'
import {
  AgentGraphIntentClaimConflictError,
  AgentGraphScheduleClosedError,
  AgentGraphScheduleRevisionConflictError,
  GraphControlError,
} from './errors.ts'
import type {
  AgentGraphControlSnapshot,
  AgentGraphIntentAdmissionState,
  AgentGraphIntentAdmissionTransition,
  AgentGraphIntentClaim,
  AgentGraphIntentClaimRequest,
  AgentGraphIntentClaimResult,
  AgentGraphOperatorBinding,
  AgentGraphOperatorProvision,
  AgentGraphOperatorProvisionRequest,
  AgentGraphOperatorProvisionResult,
  AgentGraphScheduleUpdate,
  AgentGraphScheduleUpdateRequest,
  AgentGraphScheduleUpdateResult,
  AgentGraphSupervisorWakeAttemptRecord,
  AgentGraphSupervisorWakeRecord,
  AgentGraphSupervisorWakeStatus,
  BeginAgentGraphSupervisorWakeAttemptRequest,
  ClaimAgentGraphSupervisorWakeRequest,
  CompleteAgentGraphSupervisorWakeAttemptRequest,
  SupersedeAgentGraphSupervisorWakesRequest,
} from './types.ts'

export const AGENT_GRAPH_CONTROL_UNIT_NAME = 'agent_graph'
export const AGENT_GRAPH_CONTROL_UNIT_VERSION = 1

const UNIT_TABLES = ['schedule', 'claims', 'provisions', 'operator_bindings', 'wakes', 'wake_attempts'] as const

/** Claim row as stored: the public claim plus its durable admission status. */
export interface AgentGraphIntentClaimRecord extends AgentGraphIntentClaim {
  readonly admissionStatus: AgentGraphIntentAdmissionState
  readonly cancellationReason?: string
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function mustRecord(value: unknown, slot: string): Record<string, unknown> {
  if (!isRecord(value)) {
    throw new GraphControlError('malformed-state', `kv unit '${AGENT_GRAPH_CONTROL_UNIT_NAME}' holds non-record ${slot}`)
  }
  return value
}

function claimKey(graphId: string, intentId: string): string {
  return `${graphId}:${intentId}`
}

function attemptKey(wakeId: string, attemptId: string): string {
  return `${wakeId}:${attemptId}`
}

/** Run `fn` as the single write chain on this unit (callers must not hold the lock). */
function withLock(tail: Promise<unknown>, fn: () => Promise<unknown>): Promise<unknown> {
  return tail.then(fn, fn)
}

export class GraphControlStore {  /** The unit descriptor callers open with `storage.backend.<name>.kv.open(descriptor)`. */
  static readonly descriptor = {
    name: AGENT_GRAPH_CONTROL_UNIT_NAME,
    version: AGENT_GRAPH_CONTROL_UNIT_VERSION,
    tables: UNIT_TABLES,
    hasGlobal: false,
  }

  private constructor(private readonly kv: KvUnit) { }

  /** Open the investigation-free store over an already-opened unit. */
  static async open(kv: KvUnit): Promise<GraphControlStore> {
    const store = new GraphControlStore(kv)
    await store.hydrate()
    return store
  }

  /* ------------------------- in-memory mirror ------------------------- */

  private scheduleByUpdate = new Map<string, AgentGraphScheduleUpdate>()
  private scheduleByGraph = new Map<string, AgentGraphScheduleUpdate[]>()
  private scheduleBySource = new Map<string, AgentGraphScheduleUpdate>()
  private claims = new Map<string, AgentGraphIntentClaimRecord>()
  private claimsByGraph = new Map<string, AgentGraphIntentClaimRecord[]>()
  private claimByTurn = new Map<string, string>()
  private claimByRun = new Map<string, string>()
  private provisions = new Map<string, AgentGraphOperatorProvision>()
  private provisionsByGraph = new Map<string, AgentGraphOperatorProvision[]>()
  private provisionByWork = new Map<string, string>()
  private bindings = new Map<string, AgentGraphOperatorBinding>()
  private bindingsByGraph = new Map<string, AgentGraphOperatorBinding[]>()
  private bindingByWork = new Map<string, string>()
  private wakes = new Map<string, AgentGraphSupervisorWakeRecord>()
  private attempts = new Map<string, AgentGraphSupervisorWakeAttemptRecord>()
  private lockTail: Promise<unknown> = Promise.resolve()

  /** Chain one mutation onto the write chain and keep the chain alive. */
  private locked<T>(fn: () => Promise<T>): Promise<T> {
    const next = withLock(this.lockTail, () => fn())
    this.lockTail = next.then(
      () => undefined,
      () => undefined,
    )
    return next as Promise<T>
  }

  private async hydrate(): Promise<void> {
    const snapshot = await this.kv.loadAll()
    const tables = snapshot.tables
    for (const [key, value] of Object.entries(tables['schedule'] ?? {})) {
      const update = mustRecord(value, `schedule row '${key}'`) as unknown as AgentGraphScheduleUpdate
      this.scheduleByUpdate.set(key, update)
    }
    for (const [key, value] of Object.entries(tables['claims'] ?? {})) {
      const claim = mustRecord(value, `claim row '${key}'`) as unknown as AgentGraphIntentClaimRecord
      this.claims.set(key, claim)
    }
    for (const [key, value] of Object.entries(tables['provisions'] ?? {})) {
      const provision = mustRecord(value, `provision row '${key}'`) as unknown as AgentGraphOperatorProvision
      this.provisions.set(key, provision)
    }
    for (const [key, value] of Object.entries(tables['operator_bindings'] ?? {})) {
      const binding = mustRecord(value, `operator binding row '${key}'`) as unknown as AgentGraphOperatorBinding
      this.bindings.set(key, binding)
    }
    for (const [key, value] of Object.entries(tables['wakes'] ?? {})) {
      const wake = mustRecord(value, `wake row '${key}'`) as unknown as AgentGraphSupervisorWakeRecord
      this.wakes.set(key, wake)
    }
    for (const [key, value] of Object.entries(tables['wake_attempts'] ?? {})) {
      const attempt = mustRecord(value, `attempt row '${key}'`) as unknown as AgentGraphSupervisorWakeAttemptRecord
      this.attempts.set(key, attempt)
    }
    this.rebuildIndexes()
  }

  /** Derived indexes only — the authoritative rows above are the heal source. */
  private rebuildIndexes(): void {
    this.scheduleByGraph = new Map()
    this.scheduleBySource = new Map()
    this.claimsByGraph = new Map()
    this.claimByTurn = new Map()
    this.claimByRun = new Map()
    this.provisionsByGraph = new Map()
    this.provisionByWork = new Map()
    this.bindingsByGraph = new Map()
    this.bindingByWork = new Map()
    for (const update of this.scheduleByUpdate.values()) {
      const list = this.scheduleByGraph.get(update.graphId) ?? []
      list.push(update)
      this.scheduleByGraph.set(update.graphId, list)
      this.scheduleBySource.set(`${update.graphId}:${update.source.sessionId}:${update.source.runId}:${update.source.toolCallId}`, update)
    }
    for (const list of this.scheduleByGraph.values()) list.sort((a, b) => a.revision - b.revision)
    for (const claim of this.claims.values()) {
      const list = this.claimsByGraph.get(claim.graphId) ?? []
      list.push(claim)
      this.claimsByGraph.set(claim.graphId, list)
      this.claimByTurn.set(`${claim.targetSessionId}:${claim.targetTurnId}`, claim.claimId)
      this.claimByRun.set(`${claim.targetSessionId}:${claim.targetRunId}`, claim.claimId)
    }
    for (const provision of this.provisions.values()) {
      const list = this.provisionsByGraph.get(provision.graphId) ?? []
      list.push(provision)
      this.provisionsByGraph.set(provision.graphId, list)
      this.provisionByWork.set(`${provision.graphId}:${provision.workId}`, provision.provisionId)
    }
    for (const binding of this.bindings.values()) {
      const list = this.bindingsByGraph.get(binding.graphId) ?? []
      list.push(binding)
      this.bindingsByGraph.set(binding.graphId, list)
      this.bindingByWork.set(`${binding.graphId}:${binding.workId}`, binding.provisionId)
    }
  }

  private currentRevision(graphId: string): number {
    const list = this.scheduleByGraph.get(graphId) ?? []
    const last = list[list.length - 1]
    return last === undefined ? 0 : last.revision
  }

  private isClosed(graphId: string): boolean {
    const list = this.scheduleByGraph.get(graphId) ?? []
    for (let i = list.length - 1; i >= 0; i -= 1) {
      const update = list[i]
      if (update !== undefined && update.finish !== undefined) return true
    }
    return false
  }

  private async put(table: string, key: string, value: unknown): Promise<void> {
    await this.kv.putRecord(table, key, value)
  }

  /* --------------------------- schedule log --------------------------- */

  /**
   * Append one idempotent supervisor decision. Revision assignment, source
   * idempotency, and the no-add-work-with-finish invariant are one atomic
   * decision under the write chain.
   */
  commitScheduleUpdate(request: AgentGraphScheduleUpdateRequest): Promise<AgentGraphScheduleUpdateResult> {
    return this.locked(async () => {
      if (request.finish !== undefined && request.addWork.length > 0) {
        throw new GraphControlError(
          'schedule-update-conflict',
          `agent graph ${request.graphId}: finish cannot be combined with add_work`,
        )
      }
      const existing = this.scheduleByUpdate.get(request.updateId)
      if (existing !== undefined) {
        if (existing.updateFingerprint !== request.updateFingerprint) {
          throw new GraphControlError(
            'schedule-update-conflict',
            `agent graph ${request.graphId}: update ${request.updateId} replayed with a different payload`,
          )
        }
        return { update: existing, created: false }
      }
      const sourceKey = `${request.graphId}:${request.source.sessionId}:${request.source.runId}:${request.source.toolCallId}`
      const bySource = this.scheduleBySource.get(sourceKey)
      if (bySource !== undefined && bySource.updateId !== request.updateId) {
        throw new GraphControlError(
          'schedule-update-conflict',
          `agent graph ${request.graphId}: source ${sourceKey} already committed a different update`,
        )
      }
      const update: AgentGraphScheduleUpdate = {
        ...request,
        revision: this.currentRevision(request.graphId) + 1,
        committedAt: Date.now(),
      }
      this.scheduleByUpdate.set(update.updateId, update)
      this.scheduleBySource.set(sourceKey, update)
      const list = this.scheduleByGraph.get(update.graphId) ?? []
      list.push(update)
      this.scheduleByGraph.set(update.graphId, list)
      await this.put('schedule', update.updateId, update)
      return { update, created: true }
    })
  }

  listScheduleUpdates(graphId: string): Promise<AgentGraphScheduleUpdate[]> {
    return Promise.resolve([...(this.scheduleByGraph.get(graphId) ?? [])])
  }

  readScheduleUpdate(graphId: string, updateId: string): Promise<AgentGraphScheduleUpdate | undefined> {
    const update = this.scheduleByUpdate.get(updateId)
    return Promise.resolve(update !== undefined && update.graphId === graphId ? update : undefined)
  }

  /* ------------------------ intent claims (CAS) ----------------------- */

  /**
   * Claim one runnable intent, linearized against `expectedRevision`.
   * Exactly-once: preallocated turn/run identity is written before any runtime
   * action; a retry of the same claim observes the persisted identity.
   */
  claimIntentAtScheduleRevision(
    request: AgentGraphIntentClaimRequest,
    expectedRevision: number,
  ): Promise<AgentGraphIntentClaimResult> {
    return this.locked(async () => {
      const current = this.currentRevision(request.graphId)
      if (current !== expectedRevision) {
        throw new AgentGraphScheduleRevisionConflictError(request.graphId, expectedRevision, current)
      }
      const existing = this.claims.get(claimKey(request.graphId, request.intentId))
      if (existing !== undefined) {
        if (
          existing.claimId !== request.claimId ||
          existing.intentFingerprint !== request.intentFingerprint ||
          existing.readinessContextFingerprint !== request.readinessContextFingerprint ||
          existing.targetSessionId !== request.targetSessionId ||
          existing.targetRunId !== request.targetRunId
        ) {
          throw new AgentGraphIntentClaimConflictError(
            `agent graph ${request.graphId}: intent ${request.intentId} already claimed by a different activation`,
          )
        }
        return { claim: existing, created: false }
      }
      if (this.isClosed(request.graphId)) {
        throw new AgentGraphScheduleClosedError(request.graphId)
      }
      const turnKey = `${request.targetSessionId}:${request.targetTurnId}`
      const runKey = `${request.targetSessionId}:${request.targetRunId}`
      const turnHeld = this.claimByTurn.get(turnKey)
      const runHeld = this.claimByRun.get(runKey)
      if (turnHeld !== undefined || runHeld !== undefined) {
        throw new AgentGraphIntentClaimConflictError(
          `agent graph ${request.graphId}: claim ${request.claimId} reuses an activation identity held by ${turnHeld ?? runHeld ?? 'unknown'
          }`,
        )
      }
      const claim: AgentGraphIntentClaimRecord = { ...request, claimedAt: Date.now(), admissionStatus: 'claimed' }
      this.claims.set(claimKey(request.graphId, request.intentId), claim)
      this.claimByTurn.set(turnKey, claim.claimId)
      this.claimByRun.set(runKey, claim.claimId)
      const list = this.claimsByGraph.get(request.graphId) ?? []
      list.push(claim)
      this.claimsByGraph.set(request.graphId, list)
      await this.put('claims', claimKey(request.graphId, request.intentId), claim)
      return { claim, created: true }
    })
  }

  /** Keep the by-graph claim index in step with a mutated claim row (begin/cancel). */
  #patchClaimsByGraph(updated: AgentGraphIntentClaimRecord): void {
    const list = this.claimsByGraph.get(updated.graphId)
    if (list === undefined) return
    const index = list.findIndex(claim => claim.claimId === updated.claimId)
    if (index >= 0) list[index] = updated
  }

  /** Plain claim at the current revision (Maka `claimAgentGraphIntent`). */
  claimIntent(request: AgentGraphIntentClaimRequest): Promise<AgentGraphIntentClaimResult> {
    return this.claimIntentAtScheduleRevision(request, this.currentRevision(request.graphId))
  }

  readAgentGraphIntentClaim(graphId: string, intentId: string): Promise<AgentGraphIntentClaimRecord | undefined> {
    return Promise.resolve(this.claims.get(claimKey(graphId, intentId)))
  }

  listAgentGraphIntentClaims(graphId?: string): Promise<AgentGraphIntentClaimRecord[]> {
    if (graphId !== undefined) return Promise.resolve([...(this.claimsByGraph.get(graphId) ?? [])])
    return Promise.resolve([...this.claims.values()])
  }

  /** `claimed → executing`, revision-conditional; a cancelled claim short-circuits. */
  beginAgentGraphIntentExecutionAtScheduleRevision(
    graphId: string,
    intentId: string,
    expectedRevision: number,
  ): Promise<AgentGraphIntentAdmissionTransition> {
    return this.locked(async () => {
      const current = this.currentRevision(graphId)
      if (current !== expectedRevision) {
        throw new AgentGraphScheduleRevisionConflictError(graphId, expectedRevision, current)
      }
      const claim = this.claims.get(claimKey(graphId, intentId))
      if (claim === undefined) {
        throw new GraphControlError('intent-not-found', `agent graph ${graphId}: intent ${intentId} has no durable claim`)
      }
      if (claim.admissionStatus === 'cancelled') {
        return { state: 'cancelled', previousState: 'cancelled', changed: false }
      }
      if (claim.admissionStatus === 'executing') {
        return { state: 'executing', previousState: 'executing', changed: false }
      }
      const updated: AgentGraphIntentClaimRecord = { ...claim, admissionStatus: 'executing' }
      this.claims.set(claimKey(graphId, intentId), updated)
      this.#patchClaimsByGraph(updated)
      await this.put('claims', claimKey(graphId, intentId), updated)
      return { state: 'executing', previousState: 'claimed', changed: true }
    })
  }

  cancelAgentGraphIntentExecution(graphId: string, intentId: string, reason: string): Promise<AgentGraphIntentAdmissionTransition> {
    return this.locked(async () => {
      const claim = this.claims.get(claimKey(graphId, intentId))
      if (claim === undefined) {
        throw new GraphControlError('intent-not-found', `agent graph ${graphId}: intent ${intentId} has no durable claim`)
      }
      if (claim.admissionStatus === 'cancelled') {
        return { state: 'cancelled', previousState: 'cancelled', changed: false }
      }
      const updated: AgentGraphIntentClaimRecord = { ...claim, admissionStatus: 'cancelled', cancellationReason: reason }
      this.claims.set(claimKey(graphId, intentId), updated)
      this.#patchClaimsByGraph(updated)
      await this.put('claims', claimKey(graphId, intentId), updated)
      return { state: 'cancelled', previousState: claim.admissionStatus, changed: true }
    })
  }

  /* ------------------------- operator provisions ---------------------- */

  /**
   * Monotonic operator addition, linearized against `expectedScheduleRevision`.
   * Deterministic provision/operator ids make retries adopt the same operator;
   * the provision row is written atomically with the child-session binding by
   * the caller (the graph-control unit keeps only the graph-side half here).
   */
  provisionOperator(request: AgentGraphOperatorProvisionRequest): Promise<AgentGraphOperatorProvisionResult> {
    return this.locked(async () => {
      const current = this.currentRevision(request.graphId)
      if (current !== request.expectedScheduleRevision) {
        throw new AgentGraphScheduleRevisionConflictError(request.graphId, request.expectedScheduleRevision, current)
      }
      const existing = this.provisions.get(request.provisionId)
      if (existing !== undefined) {
        if (
          existing.operatorId !== request.operatorId ||
          existing.provisionFingerprint !== request.provisionFingerprint ||
          existing.targetSessionId !== request.targetSessionId
        ) {
          throw new GraphControlError(
            'provision-conflict',
            `agent graph ${request.graphId}: provision ${request.provisionId} replayed with a different binding`,
          )
        }
        return { provision: existing, created: false }
      }
      const byWork = this.provisionByWork.get(`${request.graphId}:${request.workId}`)
      if (byWork !== undefined && byWork !== request.provisionId) {
        throw new GraphControlError(
          'provision-conflict',
          `agent graph ${request.graphId}: work ${request.workId} already provisioned as ${byWork}`,
        )
      }
      if (this.isClosed(request.graphId)) {
        throw new AgentGraphScheduleClosedError(request.graphId)
      }
      const { expectedScheduleRevision: _expected, ...rest } = request
      const provision: AgentGraphOperatorProvision = { ...rest, provisionedAt: Date.now() }
      this.provisions.set(provision.provisionId, provision)
      this.provisionByWork.set(`${provision.graphId}:${provision.workId}`, provision.provisionId)
      const list = this.provisionsByGraph.get(provision.graphId) ?? []
      list.push(provision)
      this.provisionsByGraph.set(provision.graphId, list)
      await this.put('provisions', provision.provisionId, provision)
      return { provision, created: true }
    })
  }

  listOperatorProvisions(graphId: string): Promise<AgentGraphOperatorProvision[]> {
    return Promise.resolve([...(this.provisionsByGraph.get(graphId) ?? [])])
  }

  readOperatorProvision(provisionId: string): Promise<AgentGraphOperatorProvision | undefined> {
    return Promise.resolve(this.provisions.get(provisionId))
  }

  /* ----------------------- operator worktree bindings ----------------- */
  /**
   * Persist one operator worktree binding, keyed by `provisionId`. Re-binding
   * the same provision with the SAME lease id adopts the existing row (the
   * original `boundAt` is kept); re-binding with a DIFFERENT lease id is
   * rejected — a provision owns exactly one worktree lease.
   */
  bindOperatorWorktree(binding: AgentGraphOperatorBinding): Promise<void> {
    return this.locked(async () => {
      const existing = this.bindings.get(binding.provisionId)
      if (existing !== undefined && existing.leaseId !== binding.leaseId) {
        throw new GraphControlError(
          'binding-conflict',
          `agent graph ${binding.graphId}: provision ${binding.provisionId} is bound to lease ${existing.leaseId}, cannot rebind to ${binding.leaseId}`,
        )
      }
      const row: AgentGraphOperatorBinding =
        existing !== undefined
          ? { ...binding, boundAt: existing.boundAt }
          : binding
      this.bindings.set(row.provisionId, row)
      const list = this.bindingsByGraph.get(row.graphId) ?? []
      if (existing === undefined) list.push(row)
      else {
        const index = list.findIndex(item => item.provisionId === row.provisionId)
        if (index >= 0) list[index] = row
      }
      this.bindingsByGraph.set(row.graphId, list)
      this.bindingByWork.set(`${row.graphId}:${row.workId}`, row.provisionId)
      await this.put('operator_bindings', row.provisionId, row)
    })
  }

  readOperatorBinding(provisionId: string): Promise<AgentGraphOperatorBinding | undefined> {
    return Promise.resolve(this.bindings.get(provisionId))
  }

  readOperatorBindingByWork(
    graphId: string,
    workId: string,
  ): Promise<AgentGraphOperatorBinding | undefined> {
    const provisionId = this.bindingByWork.get(`${graphId}:${workId}`)
    if (provisionId === undefined) return Promise.resolve(undefined)
    return Promise.resolve(this.bindings.get(provisionId))
  }

  listOperatorBindings(graphId?: string): Promise<AgentGraphOperatorBinding[]> {
    if (graphId !== undefined) return Promise.resolve([...(this.bindingsByGraph.get(graphId) ?? [])])
    return Promise.resolve([...this.bindings.values()])
  }

  /* --------------------------- supervisor wakes ----------------------- */

  claimSupervisorWake(request: ClaimAgentGraphSupervisorWakeRequest): Promise<{ wake: AgentGraphSupervisorWakeRecord; created: boolean }> {
    return this.locked(async () => {
      const existing = this.wakes.get(request.wakeId)
      if (existing !== undefined) return { wake: existing, created: false }
      const now = Date.now()
      const wake: AgentGraphSupervisorWakeRecord = {
        wakeId: request.wakeId,
        graphId: request.graphId,
        snapshotVersion: request.snapshotVersion,
        rootSessionId: request.rootSessionId,
        status: 'pending',
        attemptCount: 0,
        createdAt: now,
        updatedAt: now,
      }
      this.wakes.set(wake.wakeId, wake)
      await this.put('wakes', wake.wakeId, wake)
      return { wake, created: true }
    })
  }

  /**
   * Begin one delivery attempt. Acquire is refused once the wake is delivered
   * or superseded; retrying the same attempt returns the existing attempt.
   */
  beginSupervisorWakeAttempt(request: BeginAgentGraphSupervisorWakeAttemptRequest): Promise<{
    wake: AgentGraphSupervisorWakeRecord
    attempt?: AgentGraphSupervisorWakeAttemptRecord
    acquired: boolean
  }> {
    return this.locked(async () => {
      const wake = this.wakes.get(request.wakeId)
      if (wake === undefined) {
        throw new GraphControlError('wake-not-found', `agent graph ${request.graphId}: wake ${request.wakeId} not found`)
      }
      const key = attemptKey(request.wakeId, request.attemptId)
      const existingAttempt = this.attempts.get(key)
      if (existingAttempt !== undefined) {
        return { wake, attempt: existingAttempt, acquired: false }
      }
      if (wake.status === 'delivered' || wake.status === 'superseded') {
        return { wake, acquired: false }
      }
      const now = Date.now()
      const attempt: AgentGraphSupervisorWakeAttemptRecord = {
        attemptId: request.attemptId,
        wakeId: request.wakeId,
        graphId: request.graphId,
        turnId: request.turnId,
        status: 'running',
        startedAt: now,
      }
      const updated: AgentGraphSupervisorWakeRecord = {
        ...wake,
        status: 'running',
        attemptCount: wake.attemptCount + 1,
        currentAttemptId: request.attemptId,
        currentTurnId: request.turnId,
        updatedAt: now,
      }
      this.attempts.set(key, attempt)
      this.wakes.set(request.wakeId, updated)
      await this.put('wake_attempts', key, attempt)
      await this.put('wakes', request.wakeId, updated)
      return { wake: updated, attempt, acquired: true }
    })
  }

  completeSupervisorWakeAttempt(request: CompleteAgentGraphSupervisorWakeAttemptRequest): Promise<AgentGraphSupervisorWakeRecord> {
    return this.locked(async () => {
      const wake = this.wakes.get(request.wakeId)
      if (wake === undefined) {
        throw new GraphControlError('wake-not-found', `agent graph ${request.graphId}: wake ${request.wakeId} not found`)
      }
      const key = attemptKey(request.wakeId, request.attemptId)
      const attempt = this.attempts.get(key)
      if (attempt === undefined) {
        throw new GraphControlError('wake-attempt-not-found', `agent graph ${request.graphId}: attempt ${request.attemptId} not found`)
      }
      const now = Date.now()
      const updatedAttempt: AgentGraphSupervisorWakeAttemptRecord = {
        ...attempt,
        status: request.status,
        completedAt: now,
        ...(request.failureReason !== undefined ? { failureReason: request.failureReason } : {}),
      }
      const updated: AgentGraphSupervisorWakeRecord = {
        ...wake,
        status: request.status,
        updatedAt: now,
      }
      this.attempts.set(key, updatedAttempt)
      this.wakes.set(request.wakeId, updated)
      await this.put('wake_attempts', key, updatedAttempt)
      await this.put('wakes', request.wakeId, updated)
      return updated
    })
  }

  supersedeSupervisorWakes(request: SupersedeAgentGraphSupervisorWakesRequest): Promise<number> {
    return this.locked(async () => {
      const rootSet = new Set(request.rootSessionIds)
      const graphSet = request.graphIds === undefined ? undefined : new Set(request.graphIds)
      let changed = 0
      for (const wake of [...this.wakes.values()]) {
        if (!rootSet.has(wake.rootSessionId)) continue
        if (wake.status === 'delivered' || wake.status === 'superseded') continue
        if (graphSet !== undefined && !graphSet.has(wake.graphId)) continue
        const updated: AgentGraphSupervisorWakeRecord = {
          ...wake,
          status: 'superseded',
          supersededReason: request.reason,
          updatedAt: Date.now(),
        }
        this.wakes.set(wake.wakeId, updated)
        await this.put('wakes', wake.wakeId, updated)
        changed += 1
      }
      return changed
    })
  }

  readSupervisorWake(graphId: string, wakeId: string): Promise<AgentGraphSupervisorWakeRecord | undefined> {
    const wake = this.wakes.get(wakeId)
    return Promise.resolve(wake !== undefined && wake.graphId === graphId ? wake : undefined)
  }

  listSupervisorWakeAttempts(graphId: string, wakeId: string): Promise<AgentGraphSupervisorWakeAttemptRecord[]> {
    const result: AgentGraphSupervisorWakeAttemptRecord[] = []
    for (const attempt of this.attempts.values()) {
      if (attempt.wakeId === wakeId && attempt.graphId === graphId) result.push(attempt)
    }
    result.sort((a, b) => a.startedAt - b.startedAt)
    return Promise.resolve(result)
  }

  listUnsettledSupervisorWakes(): Promise<AgentGraphSupervisorWakeRecord[]> {
    return Promise.resolve([...this.wakes.values()].filter(wake => isUnsettled(wake.status)))
  }

  listRetryableSupervisorWakes(): Promise<AgentGraphSupervisorWakeRecord[]> {
    return Promise.resolve([...this.wakes.values()].filter(wake => wake.status === 'retryable_failed'))
  }

  /**
   * No-op by design: whether an interrupted wake attempt actually completed is
   * a Runtime fact (the AgentRun outcome), so the coordinator (P5) inspects
   * run facts and calls {@link completeSupervisorWakeAttempt} with
   * `retryable_failed`/`delivered`; the store never guesses.
   */
  recoverSupervisorWakes(): Promise<number> {
    return Promise.resolve(0)
  }

  /* ------------------------------ misc -------------------------------- */

  /** Diagnostics / tests: the whole authoritative state, derived indexes excluded. */
  snapshot(): Promise<AgentGraphControlSnapshot> {
    return Promise.resolve({
      scheduleUpdates: [...this.scheduleByUpdate.values()].sort((a, b) => b.revision - a.revision),
      intentClaims: [...this.claims.values()],
      operatorProvisions: [...this.provisions.values()],
      operatorBindings: [...this.bindings.values()],
      supervisorWakes: [...this.wakes.values()],
    })
  }

  async close(): Promise<void> {
    await this.kv.close()
  }
}

function isUnsettled(status: AgentGraphSupervisorWakeStatus): boolean {
  return status === 'pending' || status === 'running' || status === 'waiting_permission' || status === 'retryable_failed'
}
