/**
 * Bounded model-visible snapshot of one agent graph (Maka
 * `readToolGraphView` port). Every list is capped with explicit `omitted`
 * counts: live (requested) work is paged through the opaque cursor, terminal
 * work and stopped targets are tailed, records are tailed with truncated
 * summaries, readiness intents are tailed. Nothing here reads or writes the
 * store — the controller supplies the already-folded inputs.
 * @module
 */

import type {
  AgentGraphRecord,
  AgentGraphReadinessPolicyKind,
  AgentGraphScheduleProjection,
  AgentGraphWorkStatus,
  AgentGraphReadinessSnapshotResult,
} from '@hy-sde-org/dsh-graph-stream'
import { AgentGraphInvalidInputError } from './errors.ts'

export const TOOL_VIEW_MAX_TERMINAL_WORK = 64
export const TOOL_VIEW_MAX_STOPPED_TARGETS = 64
export const TOOL_VIEW_MAX_INSTRUCTION_CHARS = 2_000
export const TOOL_VIEW_MAX_RECORDS = 64
export const TOOL_VIEW_MAX_SUMMARY_CHARS = 512
export const TOOL_VIEW_MAX_READINESS = 64
export const TOOL_VIEW_MAX_LIVE_STATE = 64

/** One work item as the model sees it: status plus a truncated instruction. */
export interface AgentGraphToolWorkView {
  readonly workId: string
  readonly target: { readonly kind: 'agent' | 'preset' | 'operator'; readonly id: string }
  readonly instruction: string
  readonly instructionTruncated: boolean
  readonly inputIds: readonly string[]
  readonly selectedResultInputs?: readonly { readonly sourceGraphId: string; readonly resultId: string }[]
  readonly replaces?: string
  readonly status: AgentGraphWorkStatus
}

export interface AgentGraphToolRecordView {
  readonly recordId: string
  readonly operatorId: string
  /** Truncated summary (the fold already bounds full text at 16 KiB). */
  readonly summary: string
  readonly summaryTruncated: boolean
  readonly facets: readonly ('message' | 'terminal' | 'runtime_fact')[]
  readonly emittedAt: number
}

export interface AgentGraphToolIntentView {
  readonly intentId: string
  readonly operatorId: string
  readonly policyKind: AgentGraphReadinessPolicyKind
  readonly inputIds: readonly string[]
  readonly triggerRecordIds: readonly string[]
}

/** Model-visible bounded snapshot. */
export interface AgentGraphToolSnapshot {
  readonly graphId: string
  readonly closed: boolean
  readonly revision: number
  readonly updateCount: number
  readonly work: readonly AgentGraphToolWorkView[]
  readonly stoppedTargets: readonly { readonly targetId: string; readonly reason: string }[]
  readonly finish?: { readonly resultIds: readonly string[]; readonly reason: string }
  readonly records: readonly AgentGraphToolRecordView[]
  readonly readiness: {
    readonly intents: readonly AgentGraphToolIntentView[]
    readonly routesCount: number
  }
  readonly omitted: {
    readonly work: number
    readonly stoppedTargets: number
    readonly records: number
    readonly partialRecords: number
    readonly readiness: number
  }
  readonly nextCursor?: string
}

/** Cursor syntax bound (Maka: identity, ≤512 chars, no control chars). */
const CURSOR_PATTERN = /^(work:|record:|intent:)/

function cursorOf(kind: 'work' | 'record' | 'intent', id: string): string {
  return `${kind}:${id}`
}

type LiveStateEntry =
  | { readonly kind: 'work'; readonly id: string; readonly cursor: string }
  | { readonly kind: 'record'; readonly id: string; readonly cursor: string }
  | { readonly kind: 'intent'; readonly id: string; readonly cursor: string }

/** Build the bounded tool snapshot from folded graph state. */
export function buildAgentGraphToolSnapshot(input: {
  readonly projection: AgentGraphScheduleProjection
  readonly records: readonly AgentGraphRecord[]
  readonly omittedPartialCount: number
  readonly readiness: AgentGraphReadinessSnapshotResult
  readonly cursor?: string
}): AgentGraphToolSnapshot {
  const entries: LiveStateEntry[] = [
    ...input.projection.work
      .filter(work => work.status === 'requested')
      .map(work => ({
        kind: 'work' as const,
        id: work.workId,
        cursor: cursorOf('work', work.workId),
      })),
    ...input.records.map(record => ({
      kind: 'record' as const,
      id: record.recordId,
      cursor: cursorOf('record', record.recordId),
    })),
    ...input.readiness.intents.map(intent => ({
      kind: 'intent' as const,
      id: intent.intentId,
      cursor: cursorOf('intent', intent.intentId),
    })),
  ]
  const page = paginateLiveState(entries, input.cursor)

  const workIds = page.workIds
  const recordIds = page.recordIds
  const intentIds = page.intentIds

  const liveWork = input.projection.work.filter(
    work => work.status === 'requested' && workIds.has(work.workId),
  )
  const terminalWork = input.projection.work.filter(work => work.status !== 'requested')
  const visibleTerminalWork = terminalWork.slice(-TOOL_VIEW_MAX_TERMINAL_WORK)
  const work = [...liveWork, ...visibleTerminalWork].map((workish) => {
    const instruction = truncateInstruction(workish.instruction)
    return {
      workId: workish.workId,
      target: { kind: workish.target.kind, id: workish.target.id },
      instruction: instruction.text,
      instructionTruncated: instruction.truncated,
      inputIds: [...workish.inputIds],
      ...(workish.selectedResultInputs !== undefined
        ? { selectedResultInputs: workish.selectedResultInputs.map(item => ({ ...item })) }
        : {}),
      ...(workish.replaces !== undefined ? { replaces: workish.replaces } : {}),
      status: workish.status,
    }
  })

  const visibleStopped = input.projection.stoppedTargets.slice(-TOOL_VIEW_MAX_STOPPED_TARGETS)
  const visibleRecords = input.records
    .filter(record => recordIds.has(record.recordId))
    .slice(-TOOL_VIEW_MAX_RECORDS)
  const visibleIntents = input.readiness.intents
    .filter(intent => intentIds.has(intent.intentId))
    .slice(-TOOL_VIEW_MAX_READINESS)

  return {
    graphId: input.projection.graphId,
    closed: input.projection.closed,
    revision: input.projection.revision,
    updateCount: input.projection.updateCount,
    work,
    stoppedTargets: visibleStopped.map(stopped => ({
      targetId: stopped.targetId,
      reason: stopped.reason,
    })),
    ...(input.projection.finish !== undefined
      ? { finish: { resultIds: [...input.projection.finish.resultIds], reason: input.projection.finish.reason } }
      : {}),
    records: visibleRecords.map((record) => {
      const summary = truncateSummary(record.summary)
      return {
        recordId: record.recordId,
        operatorId: record.operatorId,
        summary: summary.text,
        summaryTruncated: summary.truncated,
        facets: [...record.facets],
        emittedAt: record.emittedAt,
      }
    }),
    readiness: {
      intents: visibleIntents.map(intent => ({
        intentId: intent.intentId,
        operatorId: intent.operatorId,
        policyKind: intent.policyKind,
        inputIds: [...intent.inputIds],
        triggerRecordIds: [...intent.triggerRecordIds],
      })),
      routesCount: input.readiness.routesCount,
    },
    omitted: {
      work: terminalWork.length - visibleTerminalWork.length
        + input.projection.work.filter(work => work.status === 'requested' && !workIds.has(work.workId)).length,
      stoppedTargets: input.projection.stoppedTargets.length - visibleStopped.length,
      records: input.records.filter(record => !recordIds.has(record.recordId)).length,
      partialRecords: input.omittedPartialCount,
      readiness: input.readiness.intents.length - visibleIntents.length,
    },
    ...(page.nextCursor !== undefined ? { nextCursor: page.nextCursor } : {}),
  }
}

function truncateInstruction(instruction: string): { text: string; truncated: boolean } {
  if (instruction.length <= TOOL_VIEW_MAX_INSTRUCTION_CHARS) {
    return { text: instruction, truncated: false }
  }
  return {
    text: `${instruction.slice(0, TOOL_VIEW_MAX_INSTRUCTION_CHARS)}…`,
    truncated: true,
  }
}

function truncateSummary(summary: string): { text: string; truncated: boolean } {
  if (summary.length <= TOOL_VIEW_MAX_SUMMARY_CHARS) {
    return { text: summary, truncated: false }
  }
  return {
    text: `${summary.slice(0, TOOL_VIEW_MAX_SUMMARY_CHARS)}…`,
    truncated: true,
  }
}

interface AgentGraphLiveStatePage {
  readonly workIds: ReadonlySet<string>
  readonly recordIds: ReadonlySet<string>
  readonly intentIds: ReadonlySet<string>
  readonly nextCursor?: string
}

/**
 * Opaque-cursor paging over the live-state entries (requested work, records,
 * readiness intents). The cursor names the LAST visible entry; the next page
 * starts after it. An unknown cursor is a stale/invalid-input error, mirroring
 * Maka's `paginateAgentGraphLiveState`.
 */
export function paginateLiveState(
  entries: readonly LiveStateEntry[],
  cursor: string | undefined,
): AgentGraphLiveStatePage {
  if (cursor !== undefined && !CURSOR_PATTERN.test(cursor)) {
    throw new AgentGraphInvalidInputError('agent graph view cursor is stale or invalid')
  }
  const cursorIndex = cursor === undefined
    ? -1
    : entries.findIndex(entry => entry.cursor === cursor)
  if (cursor !== undefined && cursorIndex < 0) {
    throw new AgentGraphInvalidInputError('agent graph view cursor is stale or invalid')
  }
  const start = cursorIndex + 1
  const visible = entries.slice(start, start + TOOL_VIEW_MAX_LIVE_STATE)
  const lastVisible = visible.length > 0 ? visible[visible.length - 1] : undefined
  return {
    workIds: new Set(
      visible.filter((entry): entry is Extract<LiveStateEntry, { kind: 'work' }> => entry.kind === 'work')
        .map(entry => entry.id),
    ),
    recordIds: new Set(
      visible.filter((entry): entry is Extract<LiveStateEntry, { kind: 'record' }> => entry.kind === 'record')
        .map(entry => entry.id),
    ),
    intentIds: new Set(
      visible.filter((entry): entry is Extract<LiveStateEntry, { kind: 'intent' }> => entry.kind === 'intent')
        .map(entry => entry.id),
    ),
    ...(lastVisible !== undefined && start + visible.length < entries.length
      ? { nextCursor: lastVisible.cursor }
      : {}),
  }
}
