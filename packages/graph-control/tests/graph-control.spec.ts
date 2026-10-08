import { afterEach, describe, expect, it } from 'vitest'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Config, SqliteStorageBackend } from '@deepseek-ai/dsh-storage-sqlite'
import {
  AgentGraphIntentClaimConflictError,
  AgentGraphScheduleClosedError,
  AgentGraphScheduleRevisionConflictError,
  GraphControlStore,
  scheduleSupervisorWakeConformanceChecks,
} from '../src/index.ts'
import type {
  AgentGraphControlSnapshot,
  AgentGraphIntentClaimRequest,
  AgentGraphOperatorProvisionRequest,
  AgentGraphScheduleUpdateRequest,
  AgentGraphScheduleUpdateSource,
  AgentGraphSupervisorWakeRecord,
} from '../src/index.ts'

/** Ported semantics of Maka's storage specs (agent-graph-{schedule-updates,intent-claims,supervisor-wakes,epochs}) over DSH's KvUnit. */

const dirs: string[] = []
afterEach(async () => {
  for (const dir of dirs.splice(0)) await rm(dir, { recursive: true, force: true })
})

async function freshPath(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'dsh-graph-control-'))
  dirs.push(dir)
  return join(dir, 'graph.db')
}

function backendAt(path: string): SqliteStorageBackend {
  return new SqliteStorageBackend(new Config({ path }))
}

async function openStore(path: string): Promise<GraphControlStore> {
  const backend = backendAt(path)
  const unit = await backend.kv.open(GraphControlStore.descriptor)
  const store = await GraphControlStore.open(unit)
  return store
}

/* ------------------------------ fixtures ------------------------------ */

const GRAPH = 'graph_session_1'
const SOURCE: AgentGraphScheduleUpdateSource = {
  sessionId: 'root-1',
  runId: 'run-1',
  turnId: 'turn-1',
  toolCallId: 'call-1',
}

let seq = 0
function updateRequest(overrides: Partial<AgentGraphScheduleUpdateRequest> = {}): AgentGraphScheduleUpdateRequest {
  seq += 1
  const source = overrides.source ?? { ...SOURCE, toolCallId: `call-${seq}` }
  return {
    schemaVersion: 1,
    updateId: `graph_update_${seq}`,
    updateFingerprint: `fp-${seq}`,
    graphId: GRAPH,
    source,
    addWork: [
      {
        workId: `graph_work_${seq}`,
        target: { kind: 'operator', id: 'op_a' },
        instruction: `task ${seq}`,
        inputIds: [],
      },
    ],
    stop: [],
    ...overrides,
  }
}

function claimRequest(overrides: Partial<AgentGraphIntentClaimRequest> = {}): AgentGraphIntentClaimRequest {
  return {
    schemaVersion: 1,
    claimId: `graph_claim_${seq}`,
    graphId: GRAPH,
    intentId: `graph_intent_${seq}`,
    intentFingerprint: `intent-fp-${seq}`,
    readinessContextFingerprint: `ctx-fp-${seq}`,
    targetOperatorId: `graph_operator_${seq}`,
    targetSessionId: `child-${seq}`,
    targetTurnId: `turn-${seq}`,
    targetRunId: `run-${seq}`,
    ...overrides,
  }
}

function provisionRequest(overrides: Partial<AgentGraphOperatorProvisionRequest> = {}): AgentGraphOperatorProvisionRequest {
  seq += 1
  return {
    provisionId: `graph_provision_${seq}`,
    graphId: GRAPH,
    workId: `graph_work_${seq}`,
    operatorId: `graph_operator_${seq}`,
    targetSessionId: `child-${seq}`,
    initialTurnId: `turn-${seq}`,
    initialRunId: `run-${seq}`,
    provisionFingerprint: `provision-fp-${seq}`,
    edges: [],
    expectedScheduleRevision: 1,
    ...overrides,
  }
}

/* ------------------- scheduling conformance fakes ---------------------- */

/** A minimal wake row in the shape the conformance checks observe. */
function conformanceRow(overrides: Partial<AgentGraphSupervisorWakeRecord> = {}): AgentGraphSupervisorWakeRecord {
  return {
    wakeId: 'graph_wake_conformance',
    graphId: 'graph_conformance',
    snapshotVersion: 'conformance',
    rootSessionId: 'graph_conformance_root',
    status: 'pending',
    attemptCount: 0,
    createdAt: 1,
    updatedAt: 1,
    ...overrides,
  }
}

function emptySnapshot(): AgentGraphControlSnapshot {
  return { scheduleUpdates: [], intentClaims: [], operatorProvisions: [], operatorBindings: [], supervisorWakes: [] }
}

/* --------------------------- schedule updates ------------------------- */

describe('schedule updates', () => {
  it('appends revisions monotonically and recomputes the graph on reopen', async () => {
    const path = await freshPath()
    const store = await openStore(path)
    const first = await store.commitScheduleUpdate(updateRequest())
    const second = await store.commitScheduleUpdate(updateRequest())
    expect(first.created).toBe(true)
    expect(second.created).toBe(true)
    expect(first.update.revision).toBe(1)
    expect(second.update.revision).toBe(2)
    const all = await store.listScheduleUpdates(GRAPH)
    expect(all.map(update => update.revision)).toEqual([1, 2])
    await store.close()
  })

  it('is idempotent for the same update id and same payload', async () => {
    const store = await openStore(await freshPath())
    const request = updateRequest()
    const first = await store.commitScheduleUpdate(request)
    const second = await store.commitScheduleUpdate(request)
    expect(first.created).toBe(true)
    expect(second.created).toBe(false)
    expect(second.update).toEqual(first.update)
    expect(await store.listScheduleUpdates(GRAPH)).toHaveLength(1)
  })

  it('rejects replaying the same update id with a different payload', async () => {
    const store = await openStore(await freshPath())
    const request = updateRequest()
    await store.commitScheduleUpdate(request)
    await expect(store.commitScheduleUpdate({ ...request, updateFingerprint: 'other' })).rejects.toMatchObject({
      code: 'schedule-update-conflict',
    })
  })

  it('rejects a different update from the same source triple', async () => {
    const store = await openStore(await freshPath())
    const request = updateRequest()
    await store.commitScheduleUpdate(request)
    const duplicate = updateRequest({ source: request.source })
    await expect(store.commitScheduleUpdate(duplicate)).rejects.toMatchObject({ code: 'schedule-update-conflict' })
  })

  it('rejects finish combined with add_work', async () => {
    const store = await openStore(await freshPath())
    const request = updateRequest({ finish: { resultIds: ['graph_record_1'], reason: 'done' } })
    await expect(store.commitScheduleUpdate(request)).rejects.toMatchObject({ code: 'schedule-update-conflict' })
  })

  it('closure: finish closes fresh admission but existing claims stay dispatchable', async () => {
    const store = await openStore(await freshPath())
    const add = await store.commitScheduleUpdate(updateRequest())
    const claim = claimRequest()
    await store.claimIntentAtScheduleRevision(claim, add.update.revision)

    const finish = await store.commitScheduleUpdate(
      updateRequest({ addWork: [], finish: { resultIds: ['graph_record_1'], reason: 'done' } }),
    )
    expect(finish.update.revision).toBe(2)

    // Fresh admission after closure is rejected.
    await expect(
      store.claimIntentAtScheduleRevision({ ...claimRequest(), intentId: 'graph_intent_new' }, finish.update.revision),
    ).rejects.toBeInstanceOf(AgentGraphScheduleClosedError)

    // The already-claimed intent still transitions at the current revision.
    const transition = await store.beginAgentGraphIntentExecutionAtScheduleRevision(
      GRAPH,
      claim.intentId,
      finish.update.revision,
    )
    expect(transition).toEqual({ state: 'executing', previousState: 'claimed', changed: true })
  })

  it('reports the current revision to a concurrent observer correctly after stale commit', async () => {
    const store = await openStore(await freshPath())
    const first = await store.commitScheduleUpdate(updateRequest())
    await expect(store.commitScheduleUpdate(updateRequest())).resolves.toMatchObject({ created: true })
    // A waiter still holding revision 1 sees the conflict class.
    await expect(
      store.claimIntentAtScheduleRevision(claimRequest({ intentId: 'graph_intent_z' }), first.update.revision),
    ).rejects.toBeInstanceOf(AgentGraphScheduleRevisionConflictError)
  })
})

/* ---------------------------- intent claims --------------------------- */

describe('intent claims', () => {
  it('claims once and returns the same activation identity on retry', async () => {
    const store = await openStore(await freshPath())
    const add = await store.commitScheduleUpdate(updateRequest())
    const request = claimRequest()
    const first = await store.claimIntentAtScheduleRevision(request, add.update.revision)
    const second = await store.claimIntentAtScheduleRevision(request, add.update.revision)
    expect(first.created).toBe(true)
    expect(second.created).toBe(false)
    expect(second.claim).toEqual(first.claim)
    expect(first.claim.claimedAt).toBeGreaterThan(0)
  })

  it('rejects a different claim over the same intent or the same activation identity', async () => {
    const store = await openStore(await freshPath())
    const add = await store.commitScheduleUpdate(updateRequest())
    const request = claimRequest()
    await store.claimIntentAtScheduleRevision(request, add.update.revision)

    await expect(
      store.claimIntentAtScheduleRevision({ ...claimRequest(), intentId: request.intentId, claimId: 'other-claim' }, add.update.revision),
    ).rejects.toBeInstanceOf(AgentGraphIntentClaimConflictError)

    await expect(
      store.claimIntentAtScheduleRevision(
        { ...claimRequest(), intentId: 'graph_intent_other', targetRunId: request.targetRunId },
        add.update.revision,
      ),
    ).rejects.toBeInstanceOf(AgentGraphIntentClaimConflictError)
  })

  it('rejects stale-revision claims without writing anything', async () => {
    const store = await openStore(await freshPath())
    await store.commitScheduleUpdate(updateRequest())
    await expect(store.claimIntentAtScheduleRevision(claimRequest(), 0)).rejects.toBeInstanceOf(
      AgentGraphScheduleRevisionConflictError,
    )
    expect(await store.listAgentGraphIntentClaims()).toHaveLength(0)
  })

  it('transitions claimed → executing → cancelled with changed flags', async () => {
    const store = await openStore(await freshPath())
    const add = await store.commitScheduleUpdate(updateRequest())
    const request = claimRequest()
    await store.claimIntentAtScheduleRevision(request, add.update.revision)

    const begin = await store.beginAgentGraphIntentExecutionAtScheduleRevision(GRAPH, request.intentId, add.update.revision)
    expect(begin).toEqual({ state: 'executing', previousState: 'claimed', changed: true })
    // Idempotent re-begin.
    const again = await store.beginAgentGraphIntentExecutionAtScheduleRevision(GRAPH, request.intentId, add.update.revision)
    expect(again.changed).toBe(false)

    const cancel = await store.cancelAgentGraphIntentExecution(GRAPH, request.intentId, 'superseded by newer work')
    expect(cancel).toEqual({ state: 'cancelled', previousState: 'executing', changed: true })
    const stored = await store.readAgentGraphIntentClaim(GRAPH, request.intentId)
    expect(stored?.admissionStatus).toBe('cancelled')
    expect(stored?.cancellationReason).toBe('superseded by newer work')
  })
})

/* --------------------------- operator provisions ---------------------- */

describe('operator provisions', () => {
  it('adopts the same provision on retry and rejects conflicting bindings', async () => {
    const store = await openStore(await freshPath())
    const add = await store.commitScheduleUpdate(updateRequest())
    const request = provisionRequest({ expectedScheduleRevision: add.update.revision })
    const first = await store.provisionOperator(request)
    const second = await store.provisionOperator(request)
    expect(first.created).toBe(true)
    expect(second.created).toBe(false)
    expect(second.provision).toEqual(first.provision)

    await expect(store.provisionOperator({ ...request, provisionFingerprint: 'different' })).rejects.toMatchObject({
      code: 'provision-conflict',
    })
  })

  it('rejects a different provision for the same work item', async () => {
    const store = await openStore(await freshPath())
    const add = await store.commitScheduleUpdate(updateRequest())
    const request = provisionRequest({ expectedScheduleRevision: add.update.revision })
    await store.provisionOperator(request)
    await expect(
      store.provisionOperator({ ...provisionRequest(), workId: request.workId, expectedScheduleRevision: add.update.revision }),
    ).rejects.toMatchObject({ code: 'provision-conflict' })
  })

  it('is revision-conditional and closure-blocked like claims', async () => {
    const store = await openStore(await freshPath())
    const first = await store.commitScheduleUpdate(updateRequest())
    await expect(
      store.provisionOperator(provisionRequest({ expectedScheduleRevision: first.update.revision + 99 })),
    ).rejects.toBeInstanceOf(AgentGraphScheduleRevisionConflictError)

    const finish = await store.commitScheduleUpdate(
      updateRequest({ addWork: [], finish: { resultIds: [], reason: 'done' } }),
    )
    await expect(
      store.provisionOperator(provisionRequest({ expectedScheduleRevision: finish.update.revision })),
    ).rejects.toBeInstanceOf(AgentGraphScheduleClosedError)
  })
})

/* ---------------------------- supervisor wakes ------------------------ */

describe('supervisor wakes', () => {
  const WAKE = {
    graphId: GRAPH,
    wakeId: 'graph_wake_1',
    snapshotVersion: 'rev-2',
    rootSessionId: 'root-1',
  }

  it('claims a wake once, begins attempts, and delivers only after completion', async () => {
    const store = await openStore(await freshPath())
    const first = await store.claimSupervisorWake(WAKE)
    const second = await store.claimSupervisorWake(WAKE)
    expect(first.created).toBe(true)
    expect(second.created).toBe(false)
    expect(second.wake).toEqual(first.wake)

    const begun = await store.beginSupervisorWakeAttempt({
      graphId: GRAPH,
      wakeId: WAKE.wakeId,
      attemptId: 'attempt-1',
      turnId: 'turn-9',
    })
    expect(begun.acquired).toBe(true)
    expect(begun.wake.status).toBe('running')
    expect(begun.wake.attemptCount).toBe(1)

    // Same attempt id is idempotent.
    const again = await store.beginSupervisorWakeAttempt({
      graphId: GRAPH,
      wakeId: WAKE.wakeId,
      attemptId: 'attempt-1',
      turnId: 'turn-9',
    })
    expect(again.acquired).toBe(false)
    expect(again.attempt?.attemptId).toBe('attempt-1')

    const wake = await store.completeSupervisorWakeAttempt({
      graphId: GRAPH,
      wakeId: WAKE.wakeId,
      attemptId: 'attempt-1',
      status: 'delivered',
    })
    expect(wake.status).toBe('delivered')
    const attempts = await store.listSupervisorWakeAttempts(GRAPH, WAKE.wakeId)
    expect(attempts).toHaveLength(1)
    expect(attempts[0]?.status).toBe('delivered')
    expect(attempts[0]?.completedAt).toBeGreaterThan(0)
  })

  it('refuses a new attempt once the wake is delivered or superseded', async () => {
    const store = await openStore(await freshPath())
    await store.claimSupervisorWake(WAKE)
    await store.beginSupervisorWakeAttempt({ graphId: GRAPH, wakeId: WAKE.wakeId, attemptId: 'a1', turnId: 't1' })
    await store.completeSupervisorWakeAttempt({ graphId: GRAPH, wakeId: WAKE.wakeId, attemptId: 'a1', status: 'delivered' })
    const refused = await store.beginSupervisorWakeAttempt({ graphId: GRAPH, wakeId: WAKE.wakeId, attemptId: 'a2', turnId: 't2' })
    expect(refused.acquired).toBe(false)
  })

  it('supersedes unsettled wakes by root (+ optional graph filter) and counts only changes', async () => {
    const store = await openStore(await freshPath())
    await store.claimSupervisorWake(WAKE)
    await store.claimSupervisorWake({ ...WAKE, wakeId: 'graph_wake_2', graphId: 'other_graph', snapshotVersion: 'rev-1' })
    await store.claimSupervisorWake({ ...WAKE, wakeId: 'graph_wake_3', snapshotVersion: 'rev-9' })
    const delivered = await store.beginSupervisorWakeAttempt({
      graphId: GRAPH,
      wakeId: WAKE.wakeId,
      attemptId: 'a1',
      turnId: 't1',
    })
    void delivered
    await store.completeSupervisorWakeAttempt({ graphId: GRAPH, wakeId: WAKE.wakeId, attemptId: 'a1', status: 'delivered' })

    // Only the remaining unsettled graph_root wake changes; other-graph wakes are untouched by the filter.
    const changed = await store.supersedeSupervisorWakes({ rootSessionIds: ['root-1'], graphIds: [GRAPH], reason: 'agent_graph_stopped' })
    expect(changed).toBe(1)
    const woke = await store.readSupervisorWake(GRAPH, WAKE.wakeId)
    expect(woke?.status).toBe('delivered')
    const unsettled = await store.listUnsettledSupervisorWakes()
    expect(unsettled.map(wake => wake.wakeId).sort()).toEqual(['graph_wake_2'])
  })

  it('recovery never guesses: the store exposes unsettled/retryable and recovers zero', async () => {
    const store = await openStore(await freshPath())
    await store.claimSupervisorWake(WAKE)
    await store.beginSupervisorWakeAttempt({ graphId: GRAPH, wakeId: WAKE.wakeId, attemptId: 'a1', turnId: 't1' })
    await store.completeSupervisorWakeAttempt({
      graphId: GRAPH,
      wakeId: WAKE.wakeId,
      attemptId: 'a1',
      status: 'retryable_failed',
      failureReason: 'provider error',
    })
    expect(await store.listRetryableSupervisorWakes()).toHaveLength(1)
    expect(await store.recoverSupervisorWakes()).toBe(0)
  })

  it('enforces a durable attempt ceiling and exhausts a retryable wake at the cap', async () => {
    const store = await openStore(await freshPath())
    await store.claimSupervisorWake(WAKE)
    const first = await store.beginSupervisorWakeAttempt({
      graphId: GRAPH,
      wakeId: WAKE.wakeId,
      attemptId: 'a1',
      turnId: 't1',
      maxAttempts: 1,
    })
    expect(first.acquired).toBe(true)
    await store.completeSupervisorWakeAttempt({
      graphId: GRAPH,
      wakeId: WAKE.wakeId,
      attemptId: 'a1',
      status: 'retryable_failed',
      failureReason: 'provider limit',
    })
    // At the ceiling a further begin is refused without incrementing.
    const blocked = await store.beginSupervisorWakeAttempt({
      graphId: GRAPH,
      wakeId: WAKE.wakeId,
      attemptId: 'a2',
      turnId: 't2',
      maxAttempts: 1,
    })
    expect(blocked.acquired).toBe(false)
    expect(blocked.wake.attemptCount).toBe(1)
    // Exhaust makes the wake terminal and keeps it out of retryable/recovery.
    const exhausted = await store.exhaustSupervisorWake(GRAPH, WAKE.wakeId, 'provider limit')
    expect(exhausted.status).toBe('exhausted')
    expect(exhausted.failureReason).toBe('provider limit')
    expect(await store.listRetryableSupervisorWakes()).toHaveLength(0)
    expect(await store.listUnsettledSupervisorWakes()).toHaveLength(0)
    expect(await store.recoverSupervisorWakes()).toBe(0)
    // A later begin is refused; the exhaustion is idempotent.
    const after = await store.beginSupervisorWakeAttempt({
      graphId: GRAPH,
      wakeId: WAKE.wakeId,
      attemptId: 'a3',
      turnId: 't3',
    })
    expect(after.acquired).toBe(false)
    expect((await store.exhaustSupervisorWake(GRAPH, WAKE.wakeId, 'again')).status).toBe('exhausted')
  })

  it('exhaustSupervisorWake rejects a non-retryable wake and a bad reason', async () => {
    const store = await openStore(await freshPath())
    await store.claimSupervisorWake(WAKE)
    // Pending is not exhaustible.
    await expect(store.exhaustSupervisorWake(GRAPH, WAKE.wakeId, 'boom')).rejects.toThrow(/not retryable_failed/)
    await expect(store.exhaustSupervisorWake(GRAPH, WAKE.wakeId, '')).rejects.toThrow(/non-empty/)
    await expect(store.exhaustSupervisorWake(GRAPH, WAKE.wakeId, 'x'.repeat(4001))).rejects.toThrow(/at most 4000/)
    // Running is not exhaustible either.
    await store.beginSupervisorWakeAttempt({ graphId: GRAPH, wakeId: WAKE.wakeId, attemptId: 'a1', turnId: 't1' })
    await expect(store.exhaustSupervisorWake(GRAPH, WAKE.wakeId, 'boom')).rejects.toThrow(/not retryable_failed/)
  })

  it('supersede leaves exhausted wakes alone', async () => {
    const store = await openStore(await freshPath())
    await store.claimSupervisorWake(WAKE)
    await store.claimSupervisorWake({ ...WAKE, wakeId: 'graph_wake_2' })
    await store.beginSupervisorWakeAttempt({ graphId: GRAPH, wakeId: WAKE.wakeId, attemptId: 'a1', turnId: 't1' })
    await store.completeSupervisorWakeAttempt({
      graphId: GRAPH,
      wakeId: WAKE.wakeId,
      attemptId: 'a1',
      status: 'retryable_failed',
      failureReason: 'boom',
    })
    await store.exhaustSupervisorWake(GRAPH, WAKE.wakeId, 'boom')
    const changed = await store.supersedeSupervisorWakes({ rootSessionIds: ['root-1'], reason: 'shutdown' })
    expect(changed).toBe(1)
    expect((await store.readSupervisorWake(GRAPH, WAKE.wakeId))?.status).toBe('exhausted')
    expect((await store.readSupervisorWake(GRAPH, 'graph_wake_2'))?.status).toBe('superseded')
  })
})

/* -------------------------- scheduled wakes --------------------------- */

describe('scheduled wakes', () => {
  const SCHEDULE = {
    graphId: GRAPH,
    wakeId: 'graph_wake_scheduled',
    snapshotVersion: 'rev-3',
    rootSessionId: 'root-1',
  }

  it('persists dueAt/recurMs through the wakes chain and the snapshot', async () => {
    const store = await openStore(await freshPath())
    const { wake, created } = await store.scheduleSupervisorWake({ ...SCHEDULE, dueAt: 5_000, recurMs: 60_000 })
    expect(created).toBe(true)
    expect(wake).toMatchObject({ status: 'pending', attemptCount: 0, dueAt: 5_000, recurMs: 60_000 })
    expect(await store.readSupervisorWake(GRAPH, SCHEDULE.wakeId)).toMatchObject({ dueAt: 5_000, recurMs: 60_000 })
    const snapshot = await store.snapshot()
    expect(snapshot.supervisorWakes.find(wake => wake.wakeId === SCHEDULE.wakeId)).toMatchObject({
      dueAt: 5_000,
      recurMs: 60_000,
    })
    await store.close()
  })

  it('keeps a recurring schedule durable across reopen', async () => {
    const path = await freshPath()
    const store = await openStore(path)
    await store.scheduleSupervisorWake({ ...SCHEDULE, dueAt: 5_000, recurMs: 60_000 })
    await store.close()
    const reopened = await openStore(path)
    expect(await reopened.readSupervisorWake(GRAPH, SCHEDULE.wakeId)).toMatchObject({
      status: 'pending',
      dueAt: 5_000,
      recurMs: 60_000,
    })
    await reopened.close()
  })

  it('re-schedules exactly-once: the same wakeId returns the row unchanged', async () => {
    const store = await openStore(await freshPath())
    const first = await store.scheduleSupervisorWake({ ...SCHEDULE, dueAt: 5_000, recurMs: 60_000 })
    const replay = await store.scheduleSupervisorWake({ ...SCHEDULE, dueAt: 5_000, recurMs: 60_000 })
    expect(replay.created).toBe(false)
    expect(replay.wake).toEqual(first.wake)
    // A different dueAt does not rewrite the claimed row either.
    const rescheduled = await store.scheduleSupervisorWake({ ...SCHEDULE, dueAt: 9_000 })
    expect(rescheduled.created).toBe(false)
    expect(rescheduled.wake).toEqual(first.wake)
    expect(await store.readSupervisorWake(GRAPH, SCHEDULE.wakeId)).toEqual(first.wake)
    await store.close()
  })

  it('schedules a one-shot without recurMs and validates the scheduling payload', async () => {
    const store = await openStore(await freshPath())
    const oneShot = await store.scheduleSupervisorWake({ ...SCHEDULE, dueAt: 5_000 })
    expect(oneShot.created).toBe(true)
    expect(oneShot.wake.recurMs).toBeUndefined()
    await expect(
      store.scheduleSupervisorWake({ ...SCHEDULE, wakeId: 'graph_wake_bad', dueAt: Number.NaN }),
    ).rejects.toMatchObject({ code: 'malformed-state' })
    await expect(
      store.scheduleSupervisorWake({ ...SCHEDULE, wakeId: 'graph_wake_bad', dueAt: 5_000, recurMs: 0 }),
    ).rejects.toMatchObject({ code: 'malformed-state' })
    await store.close()
  })

  it('passes the scheduling conformance checks and reports a misbehaving store', async () => {
    const store = await openStore(await freshPath())
    expect(await scheduleSupervisorWakeConformanceChecks(store)).toEqual([
      { check: 'schedule-round-trip', ok: true },
      { check: 'schedule-replay-idempotent', ok: true },
    ])
    await store.close()

    // A store that refuses to create is caught by the round-trip check.
    const refused = await scheduleSupervisorWakeConformanceChecks({
      scheduleSupervisorWake: () => Promise.resolve({ wake: conformanceRow(), created: false }),
      readSupervisorWake: () => Promise.resolve(undefined),
      snapshot: () => Promise.resolve(emptySnapshot()),
    })
    expect(refused.find(result => result.check === 'schedule-round-trip')?.detail).toContain('first schedule must create')

    // A store that drops the schedule fields is caught by read and snapshot.
    const dropped = await scheduleSupervisorWakeConformanceChecks({
      scheduleSupervisorWake: request => Promise.resolve({
        wake: conformanceRow(request.recurMs === undefined ? { wakeId: request.wakeId, recurMs: 60_000 } : { wakeId: request.wakeId }),
        created: true,
      }),
      readSupervisorWake: () => Promise.resolve(undefined),
      snapshot: () => Promise.resolve(emptySnapshot()),
    })
    const roundTrip = dropped.find(result => result.check === 'schedule-round-trip')
    expect(roundTrip?.ok).toBe(false)
    expect(roundTrip?.detail).toContain('dueAt must round-trip')
    expect(roundTrip?.detail).toContain('recurMs must round-trip')
    expect(roundTrip?.detail).toContain('readSupervisorWake must return the scheduled row unchanged')
    expect(roundTrip?.detail).toContain('snapshot() must include the scheduled row')
    expect(roundTrip?.detail).toContain('no recurrence')

    // A store that rewrites rows on replay is caught by the idempotency check.
    const replays = new Map<string, number>()
    const rewritten = await scheduleSupervisorWakeConformanceChecks({
      scheduleSupervisorWake: (request) => {
        const call = (replays.get(request.wakeId) ?? 0) + 1
        replays.set(request.wakeId, call)
        if (call === 1) {
          return Promise.resolve({
            wake: conformanceRow({
              wakeId: request.wakeId,
              dueAt: request.dueAt,
              ...(request.recurMs === undefined ? {} : { recurMs: request.recurMs }),
            }),
            created: true,
          })
        }
        if (call === 2) return Promise.resolve({ wake: conformanceRow({ wakeId: request.wakeId, dueAt: 9_999 }), created: false })
        return Promise.resolve({ wake: conformanceRow({ wakeId: request.wakeId, dueAt: request.dueAt }), created: true })
      },
      readSupervisorWake: () => Promise.resolve(undefined),
      snapshot: () => Promise.resolve(emptySnapshot()),
    })
    const replay = rewritten.find(result => result.check === 'schedule-replay-idempotent')
    expect(replay?.ok).toBe(false)
    expect(replay?.detail).toContain('must return the row unchanged')
    expect(replay?.detail).toContain('must not rewrite')
  })
})

/* ------------------------ durability across reopen --------------------- */

describe('reopen durability', () => {
  it('reconstructs schedule, claims, provisions, and wakes from one medium', async () => {
    const path = await freshPath()
    const backend = backendAt(path)
    const unit = await backend.kv.open(GraphControlStore.descriptor)
    const store = await GraphControlStore.open(unit)

    const add = await store.commitScheduleUpdate(updateRequest())
    const claim = claimRequest()
    await store.claimIntentAtScheduleRevision(claim, add.update.revision)
    await store.provisionOperator(provisionRequest({ expectedScheduleRevision: add.update.revision }))
    await store.claimSupervisorWake({ graphId: GRAPH, wakeId: 'graph_wake_1', snapshotVersion: 'rev-2', rootSessionId: 'root-1' })
    await store.close()
    await backend.close()

    const reopened = await openStore(path)
    expect(await reopened.listScheduleUpdates(GRAPH)).toHaveLength(1)
    expect((await reopened.readAgentGraphIntentClaim(GRAPH, claim.intentId))?.targetRunId).toBe(claim.targetRunId)
    expect(await reopened.listOperatorProvisions(GRAPH)).toHaveLength(1)
    expect((await reopened.readSupervisorWake(GRAPH, 'graph_wake_1'))?.status).toBe('pending')
    // Derived indexes healed: a second claim over the same activation identity still conflicts.
    await expect(
      reopened.claimIntentAtScheduleRevision(
        {
          ...claimRequest(),
          intentId: 'graph_intent_other',
          targetSessionId: claim.targetSessionId,
          targetRunId: claim.targetRunId,
        },
        add.update.revision,
      ),
    ).rejects.toBeInstanceOf(AgentGraphIntentClaimConflictError)
    await reopened.close()
  })

  it('keeps the by-graph claim index in step with admission transitions', async () => {
    const path = await freshPath()
    const backend = backendAt(path)
    const store = await openStore(path)
    const add = await store.commitScheduleUpdate(updateRequest({
      addWork: [
        { workId: 'graph_work_x', target: { kind: 'operator', id: 'op_a' }, instruction: 'task x', inputIds: [] },
      ],
    }))
    const claim = claimRequest({ targetOperatorId: 'graph_operator_1', targetSessionId: 'child-1' })
    await store.claimIntentAtScheduleRevision(claim, add.update.revision)

    const transition = await store.beginAgentGraphIntentExecutionAtScheduleRevision(claim.graphId, claim.intentId, add.update.revision)
    expect(transition.state).toBe('executing')
    const listed = await store.listAgentGraphIntentClaims(claim.graphId)
    expect(listed[0]?.admissionStatus).toBe('executing')

    const cancelled = await store.cancelAgentGraphIntentExecution(claim.graphId, claim.intentId, 'user request')
    expect(cancelled.state).toBe('cancelled')
    const relisted = await store.listAgentGraphIntentClaims(claim.graphId)
    expect(relisted[0]?.admissionStatus).toBe('cancelled')

    await store.close()
    await backend.close()
  })
})
