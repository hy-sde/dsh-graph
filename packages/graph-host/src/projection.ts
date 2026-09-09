/**
 * Bounded session-visible projection of one agent graph (P6 `graph/change`
 * payload builder). Turns the controller's whole-graph snapshot into the
 * P6 contract shape: capped lists with explicit `omitted` counts, statuses
 * mapped from schedule status × claim admission × terminal record, and an
 * `updatedAt` stamp. Pure — the assembler feeds the controller snapshot in.
 * @module
 */

import type { AgentGraphControllerSnapshot } from '@hy-sde-org/dsh-tool-graph'
import { scheduledWorkIntentId } from '@hy-sde-org/dsh-graph-stream'
import type { AgentGraphIntentClaimRecord } from '@hy-sde-org/dsh-graph-control'
import type { AgentGraphScheduleWorkView } from '@hy-sde-org/dsh-graph-stream'
import type { AgentGraphRecord } from '@hy-sde-org/dsh-graph-stream'
import type {
  SessionGraphProjection,
  SessionGraphWorkEntry,
  SessionGraphWorkStatus,
} from '@hy-sde-org/dsh-graph-projection/types'

export const SESSION_PROJECTION_SCHEMA_VERSION = 1 as const

/** Instruction bound of the P6 contract: at most 300 characters. */
export const SESSION_PROJECTION_INSTRUCTION_MAX_CHARS = 300
/** Work-item cap of the emitted bounded projection. */
export const SESSION_PROJECTION_MAX_WORK = 128
/** Record cap of the emitted bounded projection (by elapsed time, records are tailed). */
export const SESSION_PROJECTION_MAX_RECORDS = 64

/** P3's terminal summary conventions (graph-executor `terminalSummary`). */
const OPERATOR_FAILED_PREFIX = '[operator failed]'
const OPERATOR_CANCELLED_PREFIX = '[operator cancelled]'

/** Build the bounded session projection from one controller snapshot. */
export function buildSessionGraphProjection(input: {
  readonly graphId: string
  readonly snapshot: AgentGraphControllerSnapshot
  readonly pendingWake: boolean
  readonly now: number
}): SessionGraphProjection {
  const projection = input.snapshot.projection
  const claimsByIntent = new Map<string, AgentGraphIntentClaimRecord>()
  for (const claim of input.snapshot.claims) claimsByIntent.set(claim.intentId, claim)
  const terminalRecordByOperator = new Map<string, AgentGraphRecord>()
  for (const record of input.snapshot.records) {
    if (!record.facets.includes('terminal')) continue
    terminalRecordByOperator.set(record.operatorId, record)
  }

  const requested = projection.work.filter(work => work.status === 'requested')
  const terminal = projection.work.filter(work => work.status !== 'requested')
  const visibleWork = [
    ...requested.slice(0, SESSION_PROJECTION_MAX_WORK),
    ...terminal.slice(-SESSION_PROJECTION_MAX_WORK),
  ]
  const visibleWorkIds = new Set(visibleWork.map(work => work.workId))
  const omittedWork = projection.work.length - visibleWorkIds.size

  const visibleRecords = input.snapshot.records.slice(-SESSION_PROJECTION_MAX_RECORDS)

  const work: SessionGraphWorkEntry[] = visibleWork.map(workish =>
    workViewOf(
      workish,
      claimsByIntent.get(scheduledWorkIntentId(input.graphId, workish.workId)),
      terminalRecordByOperator,
    ),
  )

  return {
    schemaVersion: SESSION_PROJECTION_SCHEMA_VERSION,
    graphId: input.graphId,
    status: projection.closed ? 'closed' : 'active',
    revision: projection.revision,
    closed: projection.closed,
    work,
    omitted: {
      work: omittedWork,
      records: input.snapshot.records.length - visibleRecords.length,
      inputs: projection.work
        .filter(workish => !visibleWorkIds.has(workish.workId))
        .reduce((total, workish) => total + workish.inputIds.length, 0),
    },
    pendingWake: input.pendingWake,
    updatedAt: input.now,
  }
}

function workViewOf(
  work: AgentGraphScheduleWorkView,
  claim: AgentGraphIntentClaimRecord | undefined,
  terminalRecordByOperator: ReadonlyMap<string, AgentGraphRecord>,
): SessionGraphWorkEntry {
  const operatorId = claim?.targetOperatorId
    ?? (work.target.kind === 'operator' ? work.target.id : undefined)
  const terminalRecord =
    operatorId === undefined ? undefined : terminalRecordByOperator.get(operatorId)
  return {
    workId: work.workId,
    status: workStatusOf(work, claim, terminalRecord),
    instruction: truncateInstruction(work.instruction),
    ...(operatorId !== undefined ? { operatorId } : {}),
    inputCount: work.inputIds.length,
  }
}

/**
 * Status precedence: schedule stop/supersede first, then the operator's
 * terminal record (the activation outcome the executor folded — P3 summary
 * conventions: `[operator failed] …`, `[operator cancelled]`), then the claim
 * admission state (in-flight only), then still requested.
 */
function workStatusOf(
  work: AgentGraphScheduleWorkView,
  claim: AgentGraphIntentClaimRecord | undefined,
  terminalRecord: AgentGraphRecord | undefined,
): SessionGraphWorkStatus {
  if (work.status !== 'requested') return 'stopped'
  if (terminalRecord !== undefined) {
    if (terminalRecord.summary.startsWith(OPERATOR_CANCELLED_PREFIX)) return 'stopped'
    if (terminalRecord.summary.startsWith(OPERATOR_FAILED_PREFIX)) return 'failed'
    return 'finished'
  }
  if (claim !== undefined) {
    if (claim.admissionStatus === 'executing') return 'executing'
    if (claim.admissionStatus === 'claimed') return 'claimed'
    return 'stopped'
  }
  return 'requested'
}

function truncateInstruction(instruction: string): string {
  if (instruction.length <= SESSION_PROJECTION_INSTRUCTION_MAX_CHARS) return instruction
  return `${instruction.slice(0, SESSION_PROJECTION_INSTRUCTION_MAX_CHARS - 1)}…`
}
