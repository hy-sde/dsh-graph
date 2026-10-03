/**
 * Storage conformance battery for {@link GraphControlStore} (pi-durable
 * pattern harvest: pi-durable ships `src/testing/` so every Storage backend
 * runs the identical behavioral suite).
 *
 * Every check receives a FRESH store from the caller's `open()` factory and
 * asserts with plain throws — no test-framework import — so the battery ships
 * in `dist` and any backend (today the KvUnit store, tomorrow a celled-cell
 * or D1-backed one) validates the same contract:
 *
 * ```ts
 * await assertGraphControlStoreConformance(() => openStore())
 * ```
 *
 * or, for granular reporting, iterate {@link graphControlStoreConformanceChecks}.
 * @module
 */

import type { GraphControlStore } from './store.ts'
import {
  AgentGraphIntentClaimConflictError,
  AgentGraphScheduleClosedError,
  AgentGraphScheduleRevisionConflictError,
} from './errors.ts'
import {
  type AgentGraphIntentClaimRequest,
  type AgentGraphOperatorProvisionRequest,
  type AgentGraphScheduleUpdateRequest,
  type AgentGraphScheduleUpdateSource,
} from './types.ts'

export interface GraphControlStoreConformanceCheck {
  readonly name: string
  readonly run: (store: GraphControlStore) => Promise<void>
}

/* ------------------------------- fixtures ------------------------------ */

const GRAPH = 'graph_session_1'
const ROOT = 'root-1'
const WAKE = { graphId: GRAPH, wakeId: 'graph_wake_1', snapshotVersion: 'rev-2', rootSessionId: ROOT }

function sourceOf(seq: number): AgentGraphScheduleUpdateSource {
  return { sessionId: ROOT, runId: 'run-1', turnId: 'turn-1', toolCallId: `call-${seq}` }
}

function updateRequest(seq: number, overrides: Partial<AgentGraphScheduleUpdateRequest> = {}): AgentGraphScheduleUpdateRequest {
  return {
    schemaVersion: 1,
    updateId: `graph_update_${seq}`,
    updateFingerprint: `fp-${seq}`,
    graphId: GRAPH,
    source: sourceOf(seq),
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

function claimRequest(seq: number, overrides: Partial<AgentGraphIntentClaimRequest> = {}): AgentGraphIntentClaimRequest {
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

function provisionRequest(seq: number, overrides: Partial<AgentGraphOperatorProvisionRequest> = {}): AgentGraphOperatorProvisionRequest {
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

/* ------------------------------ assertions ----------------------------- */

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(message)
}

function assertEqual(actual: unknown, expected: unknown, label: string): void {
  const left = JSON.stringify(actual)
  const right = JSON.stringify(expected)
  assert(left === right, `${label}: expected ${right}, got ${left}`)
}

/* -------------------------------- checks ------------------------------- */

/** The behavioral battery. Each check runs against its own fresh store. */
export function graphControlStoreConformanceChecks(): readonly GraphControlStoreConformanceCheck[] {
  return [
    {
      name: 'schedule.revisions_append_monotonically',
      run: async store => {
        const first = await store.commitScheduleUpdate(updateRequest(1))
        const second = await store.commitScheduleUpdate(updateRequest(2))
        assert(first.created && second.created, 'both distinct updates commit as created')
        assertEqual([first.update.revision, second.update.revision], [1, 2], 'revisions')
      },
    },
    {
      name: 'schedule.commit_idempotent_same_id_same_payload',
      run: async store => {
        const request = updateRequest(1)
        const first = await store.commitScheduleUpdate(request)
        const second = await store.commitScheduleUpdate(request)
        assert(first.created, 'first commit created')
        assert(!second.created, 'retry not created')
        assertEqual(second.update, first.update, 'retry returns the same row')
        assertEqual((await store.listScheduleUpdates(GRAPH)).length, 1, 'one row stored')
      },
    },
    {
      name: 'schedule.rejects_same_update_id_different_payload',
      run: async store => {
        const request = updateRequest(1)
        await store.commitScheduleUpdate(request)
        let code = ''
        try {
          await store.commitScheduleUpdate({ ...request, updateFingerprint: 'other' })
        } catch (error) {
          code = (error as { code?: string }).code ?? ''
        }
        assertEqual(code, 'schedule-update-conflict', 'conflict code')
      },
    },
    {
      name: 'schedule.rejects_different_update_same_source',
      run: async store => {
        const request = updateRequest(1)
        await store.commitScheduleUpdate(request)
        let conflict = false
        try {
          await store.commitScheduleUpdate(updateRequest(2, { source: request.source }))
        } catch {
          conflict = true
        }
        assert(conflict, 'same source triple with a different payload must conflict')
      },
    },
    {
      name: 'schedule.finish_rejected_with_add_work',
      run: async store => {
        let conflict = false
        try {
          await store.commitScheduleUpdate(
            updateRequest(1, { finish: { resultIds: ['graph_record_1'], reason: 'done' } }),
          )
        } catch {
          conflict = true
        }
        assert(conflict, 'finish + addWork is rejected')
      },
    },
    {
      name: 'claims.exactly_once_same_activation_identity_on_retry',
      run: async store => {
        const add = await store.commitScheduleUpdate(updateRequest(1))
        const request = claimRequest(1)
        const first = await store.claimIntentAtScheduleRevision(request, add.update.revision)
        const second = await store.claimIntentAtScheduleRevision(request, add.update.revision)
        assert(first.created, 'first claim created')
        assert(!second.created, 'retry not created')
        assertEqual(second.claim, first.claim, 'retry adopts the same activation')
      },
    },
    {
      name: 'claims.reject_conflicting_intent_or_activation_identity',
      run: async store => {
        const add = await store.commitScheduleUpdate(updateRequest(1))
        const request = claimRequest(1)
        await store.claimIntentAtScheduleRevision(request, add.update.revision)
        await expectReject(
          () =>
            store.claimIntentAtScheduleRevision(
              { ...claimRequest(1), intentId: request.intentId, claimId: 'other-claim' },
              add.update.revision,
            ),
          AgentGraphIntentClaimConflictError,
        )
        await expectReject(
          () =>
            store.claimIntentAtScheduleRevision(
              { ...claimRequest(1), intentId: 'graph_intent_other', targetRunId: request.targetRunId },
              add.update.revision,
            ),
          AgentGraphIntentClaimConflictError,
        )
      },
    },
    {
      name: 'claims.reject_stale_revision_without_writes',
      run: async store => {
        await store.commitScheduleUpdate(updateRequest(1))
        await expectReject(
          () => store.claimIntentAtScheduleRevision(claimRequest(1), 0),
          AgentGraphScheduleRevisionConflictError,
        )
        assertEqual((await store.listAgentGraphIntentClaims()).length, 0, 'nothing written')
      },
    },
    {
      name: 'claims.transition_claimed_executing_cancelled',
      run: async store => {
        const add = await store.commitScheduleUpdate(updateRequest(1))
        const request = claimRequest(1)
        await store.claimIntentAtScheduleRevision(request, add.update.revision)
        const begin = await store.beginAgentGraphIntentExecutionAtScheduleRevision(GRAPH, request.intentId, add.update.revision)
        assertEqual(begin, { state: 'executing', previousState: 'claimed', changed: true }, 'begin')
        const again = await store.beginAgentGraphIntentExecutionAtScheduleRevision(GRAPH, request.intentId, add.update.revision)
        assert(!again.changed, 're-begin idempotent')
        const cancel = await store.cancelAgentGraphIntentExecution(GRAPH, request.intentId, 'superseded')
        assertEqual(cancel, { state: 'cancelled', previousState: 'executing', changed: true }, 'cancel')
        const stored = await store.readAgentGraphIntentClaim(GRAPH, request.intentId)
        assert(stored?.admissionStatus === 'cancelled' && stored.cancellationReason === 'superseded', 'cancelled row persisted')
      },
    },
    {
      name: 'claims.rearm_replay_gate_executing_to_claimed_only',
      run: async store => {
        const add = await store.commitScheduleUpdate(updateRequest(1))
        const request = claimRequest(1)
        await store.claimIntentAtScheduleRevision(request, add.update.revision)
        // claimed → no-op (nothing to re-arm)
        const claimedRearm = await store.rearmAgentGraphIntentForReplay(GRAPH, request.intentId)
        assert(!claimedRearm.changed, 're-arm of a claimed claim is a no-op')
        await store.beginAgentGraphIntentExecutionAtScheduleRevision(GRAPH, request.intentId, add.update.revision)
        const executingRearm = await store.rearmAgentGraphIntentForReplay(GRAPH, request.intentId)
        assertEqual(executingRearm, { state: 'claimed', previousState: 'executing', changed: true }, 'executing re-arm')
        const stored = await store.readAgentGraphIntentClaim(GRAPH, request.intentId)
        assert(stored?.admissionStatus === 'claimed', 'row re-armed to claimed')
        // cancelled → never re-arms (an explicit stop stays a stop)
        await store.cancelAgentGraphIntentExecution(GRAPH, request.intentId, 'stopped by supervisor')
        const cancelledRearm = await store.rearmAgentGraphIntentForReplay(GRAPH, request.intentId)
        assert(!cancelledRearm.changed && cancelledRearm.state === 'cancelled', 'cancelled claim never re-arms')
      },
    },
    {
      name: 'provisions.adopt_on_retry_reject_conflicts',
      run: async store => {
        const add = await store.commitScheduleUpdate(updateRequest(1))
        const request = provisionRequest(1, { expectedScheduleRevision: add.update.revision })
        const first = await store.provisionOperator(request)
        const second = await store.provisionOperator(request)
        assert(first.created, 'first provision created')
        assert(!second.created, 'retry not created')
        assertEqual(second.provision, first.provision, 'retry adopts the same provision')
        let conflict = false
        try {
          await store.provisionOperator({ ...request, provisionFingerprint: 'different' })
        } catch (error) {
          conflict = (error as { code?: string }).code === 'provision-conflict'
        }
        assert(conflict, 'fingerprint conflict rejected')
      },
    },
    {
      name: 'provisions.revision_conditional_and_closure_blocked',
      run: async store => {
        const first = await store.commitScheduleUpdate(updateRequest(1))
        await expectReject(
          () => store.provisionOperator(provisionRequest(1, { expectedScheduleRevision: first.update.revision + 99 })),
          AgentGraphScheduleRevisionConflictError,
        )
        const finish = await store.commitScheduleUpdate(
          updateRequest(2, { addWork: [], finish: { resultIds: [], reason: 'done' } }),
        )
        await expectReject(
          () => store.provisionOperator(provisionRequest(2, { expectedScheduleRevision: finish.update.revision })),
          AgentGraphScheduleClosedError,
        )
      },
    },
    {
      name: 'wakes.claim_once_attempt_lifecycle_then_deliver',
      run: async store => {
        const first = await store.claimSupervisorWake(WAKE)
        const second = await store.claimSupervisorWake(WAKE)
        assert(first.created && !second.created, 'wake claimed once')
        assertEqual(second.wake, first.wake, 'retry adopts the same wake')
        const begun = await store.beginSupervisorWakeAttempt({ graphId: GRAPH, wakeId: WAKE.wakeId, attemptId: 'a1', turnId: 't1' })
        assert(begun.acquired && begun.wake.status === 'running' && begun.wake.attemptCount === 1, 'attempt acquired')
        const duplicate = await store.beginSupervisorWakeAttempt({ graphId: GRAPH, wakeId: WAKE.wakeId, attemptId: 'a1', turnId: 't1' })
        assert(!duplicate.acquired, 'same attempt id idempotent')
        const wake = await store.completeSupervisorWakeAttempt({ graphId: GRAPH, wakeId: WAKE.wakeId, attemptId: 'a1', status: 'delivered' })
        assert(wake.status === 'delivered', 'delivered only after completion')
        const attempts = await store.listSupervisorWakeAttempts(GRAPH, WAKE.wakeId)
        assertEqual(attempts.length, 1, 'one attempt row')
        assert(attempts[0]?.status === 'delivered' && (attempts[0]?.completedAt ?? 0) > 0, 'attempt completed')
      },
    },
    {
      name: 'wakes.refuse_attempt_after_terminal',
      run: async store => {
        await store.claimSupervisorWake(WAKE)
        await store.beginSupervisorWakeAttempt({ graphId: GRAPH, wakeId: WAKE.wakeId, attemptId: 'a1', turnId: 't1' })
        await store.completeSupervisorWakeAttempt({ graphId: GRAPH, wakeId: WAKE.wakeId, attemptId: 'a1', status: 'delivered' })
        const refused = await store.beginSupervisorWakeAttempt({ graphId: GRAPH, wakeId: WAKE.wakeId, attemptId: 'a2', turnId: 't2' })
        assert(!refused.acquired, 'attempt refused after delivery')
      },
    },
  ]
}

/** Expect the promise to reject with the given error class; rethrow anything else. */
async function expectReject(run: () => Promise<unknown>, errorClass: abstract new (...args: never[]) => Error): Promise<void> {
  let rejected = false
  try {
    await run()
  } catch (error) {
    rejected = true
    if (!(error instanceof errorClass)) {
      throw new Error(`expected ${errorClass.name}, got ${String(error)}`)
    }
  }
  assert(rejected, `expected ${errorClass.name} rejection, resolved instead`)
}

/**
 * Run the whole battery against stores produced by `open()` (one fresh store
 * per check; each is closed before the next). Throws an aggregate error when
 * any check fails. No test-framework dependency — usable from scripts, CI
 * probes, or another package's spec.
 */
export async function assertGraphControlStoreConformance(
  open: () => Promise<GraphControlStore>,
  options?: { checks?: readonly GraphControlStoreConformanceCheck[] },
): Promise<void> {
  const failures: string[] = []
  for (const check of options?.checks ?? graphControlStoreConformanceChecks()) {
    const store = await open()
    try {
      await check.run(store)
    } catch (error) {
      failures.push(`${check.name}: ${error instanceof Error ? error.message : String(error)}`)
    } finally {
      await store.close()
    }
  }
  if (failures.length > 0) {
    throw new Error(`graph-control store conformance failed:\n- ${failures.join('\n- ')}`)
  }
}
