/**
 * Pure work-status projection over the schedule update log (Maka
 * `projectAgentGraphSchedule`). Everything here is derived from the
 * append-only updates; nothing is stored. Validation mirrors Maka: revisions
 * contiguous from 1, no updates after a finish, no repeated work ids, wrong
 * graph ids rejected.
 * @module
 */

import { compareAgentGraphIdentity } from './identity.ts'
import type { AgentGraphScheduleUpdate } from '@hy-sde-org/dsh-graph-control'
import type {
  AgentGraphScheduleProjection,
  AgentGraphScheduleWorkView,
  AgentGraphWorkStatus,
} from './types.ts'

/**
 * Fold the schedule log into the model-visible work projection.
 * Work is `stopped` iff any stop targets it (wins over superseded);
 * `superseded` iff a later work's `replaces` names it; closed iff a finish
 * update exists.
 */
export function projectAgentGraphSchedule(
  graphId: string,
  updates: readonly AgentGraphScheduleUpdate[],
): AgentGraphScheduleProjection {
  const byRevision = [...updates].sort(
    (a, b) =>
      a.revision - b.revision ||
      compareAgentGraphIdentity(a.updateId, b.updateId),
  )
  const stoppedTargets = new Map<
    string,
    {
      targetId: string
      reason: string
      updateId: string
      revision: number
      committedAt: number
    }
  >()
  const work: AgentGraphScheduleWorkView[] = []
  const workIds = new Set<string>()
  const superseded = new Set<string>()
  let finish: AgentGraphScheduleProjection['finish']

  for (const update of byRevision) {
    if (update.graphId !== graphId) {
      throw new Error(
        `agent graph ${graphId}: update ${update.updateId} belongs to ${update.graphId}`,
      )
    }
    const index = byRevision.indexOf(update)
    if (update.revision !== index + 1) {
      throw new Error(
        `agent graph ${graphId}: non-contiguous revision ${update.revision}`,
      )
    }
    if (finish !== undefined) {
      throw new Error(
        `agent graph ${graphId}: update ${update.updateId} after finish`,
      )
    }
    for (const item of update.addWork) {
      if (workIds.has(item.workId)) {
        throw new Error(
          `agent graph ${graphId}: update repeats work ${item.workId}`,
        )
      }
      workIds.add(item.workId)
      if (item.replaces !== undefined) superseded.add(item.replaces)
      work.push({
        ...item,
        status: 'requested',
        updateId: update.updateId,
        revision: update.revision,
        committedAt: update.committedAt,
      })
    }
    for (const stopped of update.stop) {
      stoppedTargets.set(stopped.targetId, {
        targetId: stopped.targetId,
        reason: stopped.reason,
        updateId: update.updateId,
        revision: update.revision,
        committedAt: update.committedAt,
      })
    }
    if (update.finish !== undefined) {
      finish = {
        resultIds: [...update.finish.resultIds],
        reason: update.finish.reason,
        updateId: update.updateId,
        revision: update.revision,
        committedAt: update.committedAt,
      }
    }
  }

  for (const item of work) {
    const status = stoppedTargets.has(item.workId)
      ? ('stopped' as const)
      : superseded.has(item.workId)
        ? ('superseded' as const)
        : ('requested' as const)
    item.status = status
  }
  const lastUpdate = byRevision[byRevision.length - 1]

  return {
    schemaVersion: 1,
    graphId,
    closed: finish !== undefined,
    revision: lastUpdate?.revision ?? 0,
    updateCount: byRevision.length,
    work,
    stoppedTargets: [...stoppedTargets.values()].sort(
      (a, b) =>
        a.revision - b.revision ||
        compareAgentGraphIdentity(a.targetId, b.targetId),
    ),
    ...(finish !== undefined ? { finish } : {}),
  }
}

export function workStatusOf(
  workId: string,
  projection: AgentGraphScheduleProjection,
): AgentGraphWorkStatus {
  for (const item of projection.work) {
    if (item.workId === workId) return item.status
  }
  return 'requested'
}
