/**
 * Schedule-update compilation for `update_agent_graph` (Maka
 * `compileAgentGraphScheduleUpdate` port).
 *
 * The Maka preprocessors are ported verbatim in spirit: `cleanUpdateInput` and
 * `cleanAddWorkInput` are discriminator-tolerant — a provider-filled
 * `operation`/`targetKind` selects one payload and the unrelated identity
 * fields are dropped; unknown keys are never read (the `.strip()` effect) so
 * dangling provider-fill garbage cannot reach the store. Bounds mirror Maka's
 * constants. Deterministic ids (updateId from the source triple, per-entry
 * workId, semantic fingerprint) make retries idempotent end-to-end.
 * @module
 */

import {
  graphUpdateId,
  MAX_ADD_WORK,
  MAX_INPUT_IDS,
  MAX_SCHEDULE_INSTRUCTION_LENGTH,
  MAX_SELECTED_RESULT_INPUTS,
  MAX_WORK_STOPS,
  type AgentGraphScheduleFinish,
  type AgentGraphScheduleStop,
  type AgentGraphScheduleUpdateRequest,
  type AgentGraphScheduleUpdateSource,
  type AgentGraphSelectedResultInput,
  type AgentGraphScheduledWork,
  type AgentGraphWorkTarget,
} from '@hy-sde-org/dsh-graph-control'
import { stableHash, stableHash32, scheduledWorkIntentId } from '@hy-sde-org/dsh-graph-stream'
import type { AgentGraphWorkStatus } from '@hy-sde-org/dsh-graph-stream'
import { AgentGraphInvalidInputError } from './errors.ts'

export const AGENT_GRAPH_TOOL_UPDATE_SCHEMA_VERSION = 1 as const
/** Maka `AGENT_GRAPH_SCHEDULE_MAX_REASON_CHARS` (stop/finish reasons). */
export const MAX_SCHEDULE_REASON_CHARS = 4_000
/** Maka `AGENT_GRAPH_SCHEDULE_MAX_RESULT_IDS` (finish selection). */
export const MAX_SCHEDULE_RESULT_IDS = 64

/* ------------------------------ tool input ---------------------------- */

export type AgentGraphToolOperation = 'add_work' | 'stop' | 'finish'
export type AgentGraphToolTargetKind = 'new_agent' | 'new_preset' | 'existing_operator'

export interface AgentGraphToolSelectedResultInput {
  readonly sourceGraphId: string
  readonly resultId: string
}

export interface AgentGraphToolAddWork {
  /** Explicit target discriminator; unrelated identity fields are ignored. */
  readonly targetKind?: AgentGraphToolTargetKind
  /** Legacy built-in agent id for new graph work (kind 'agent'). */
  readonly agentId?: string
  /** User-approved subagent preset id for new graph work (kind 'preset'). */
  readonly subagentId?: string
  /** Runtime id of an EXISTING graph operator for follow-up work (kind 'operator'). */
  readonly operatorId?: string
  readonly instruction: string
  readonly inputIds?: readonly string[]
  readonly selectedResultInputs?: readonly AgentGraphToolSelectedResultInput[]
  /** Existing work superseded by this work item. */
  readonly replaces?: string
  /** `none` ignores a provider-filled `replaces`; `replace` requires one. */
  readonly replacementMode?: 'none' | 'replace'
  /** Optional explicit work id (normally derived deterministically from the update). */
  readonly workId?: string
  [key: string]: unknown
}

export interface AgentGraphToolStop {
  readonly targetId: string
  readonly reason: string
}

export interface AgentGraphToolFinish {
  readonly resultIds: readonly string[]
  readonly reason: string
}

/** Model input of `update_agent_graph`. Unknown keys are ignored (strip). */
export interface UpdateAgentGraphToolInput {
  readonly graphId: string
  /** Explicit operation discriminator; unrelated provider-filled payloads are ignored. */
  readonly operation?: AgentGraphToolOperation
  readonly addWork?: readonly AgentGraphToolAddWork[]
  readonly stop?: readonly AgentGraphToolStop[]
  readonly finish?: AgentGraphToolFinish
  /** Stable caller-provided key folded into the source triple; a retried identical update with the same key dedupes at the store. */
  readonly idempotencyKey?: string
  [key: string]: unknown
}

/* ---------------------------- preprocessors --------------------------- */

/**
 * Discriminator-tolerant cleaning of one add-work entry (Maka
 * `cleanAddWorkInput`): `targetKind` selects the one identity field; with
 * `new_agent`/`new_preset`/`existing_operator` the unrelated identity fields
 * are dropped. `replacementMode: 'none'` drops `replaces`.
 */
export function cleanAddWorkInput(input: AgentGraphToolAddWork): AgentGraphToolAddWork {
  const identity = input.targetKind === 'new_agent'
    ? { targetKind: 'new_agent' as const, ...(input.agentId !== undefined ? { agentId: input.agentId } : {}) }
    : input.targetKind === 'new_preset'
      ? { targetKind: 'new_preset' as const, ...(input.subagentId !== undefined ? { subagentId: input.subagentId } : {}) }
      : input.targetKind === 'existing_operator'
        ? { targetKind: 'existing_operator' as const, ...(input.operatorId !== undefined ? { operatorId: input.operatorId } : {}) }
        : {
          ...(input.agentId !== undefined ? { agentId: input.agentId } : {}),
          ...(input.subagentId !== undefined ? { subagentId: input.subagentId } : {}),
          ...(input.operatorId !== undefined ? { operatorId: input.operatorId } : {}),
        }
  return {
    instruction: input.instruction,
    ...identity,
    ...(input.replacementMode !== undefined ? { replacementMode: input.replacementMode } : {}),
    ...(input.replacementMode === 'none' || input.replaces === undefined
      ? {}
      : { replaces: input.replaces }),
    ...(input.workId !== undefined ? { workId: input.workId } : {}),
    ...(input.inputIds !== undefined ? { inputIds: [...input.inputIds] } : {}),
    ...(input.selectedResultInputs !== undefined
      ? { selectedResultInputs: input.selectedResultInputs.map(item => ({ ...item })) }
      : {}),
  }
}

/** Discriminator-tolerant cleaning of the update payload (Maka `cleanUpdateInput`). */
export function cleanUpdateInput(input: UpdateAgentGraphToolInput): UpdateAgentGraphToolInput {
  if (input.operation === 'add_work') {
    return {
      graphId: input.graphId,
      operation: input.operation,
      ...(input.addWork !== undefined ? { addWork: [...input.addWork] } : {}),
      ...(input.idempotencyKey !== undefined ? { idempotencyKey: input.idempotencyKey } : {}),
    }
  }
  if (input.operation === 'stop') {
    return {
      graphId: input.graphId,
      operation: input.operation,
      ...(input.stop !== undefined ? { stop: [...input.stop] } : {}),
      ...(input.idempotencyKey !== undefined ? { idempotencyKey: input.idempotencyKey } : {}),
    }
  }
  if (input.operation === 'finish') {
    return {
      graphId: input.graphId,
      operation: input.operation,
      ...(input.finish !== undefined ? { finish: { ...input.finish } } : {}),
      ...(input.idempotencyKey !== undefined ? { idempotencyKey: input.idempotencyKey } : {}),
    }
  }
  return {
    graphId: input.graphId,
    ...(input.addWork !== undefined ? { addWork: [...input.addWork] } : {}),
    ...(input.stop !== undefined ? { stop: [...input.stop] } : {}),
    ...(input.finish !== undefined ? { finish: { ...input.finish } } : {}),
    ...(input.idempotencyKey !== undefined ? { idempotencyKey: input.idempotencyKey } : {}),
  }
}

/* ------------------------------ validation ---------------------------- */

const CONTROL_CHARS = /[\u0000-\u001f\u007f]/
const TEXT_CONTROL_CHARS = /[\u0000\u0008\u000b\u000c\u000e-\u001f\u007f]/

function requireIdentity(value: string | undefined, name: string): string {
  if (
    typeof value !== 'string' ||
    value.length === 0 ||
    value.length > 256 ||
    value.trim() !== value ||
    CONTROL_CHARS.test(value)
  ) {
    throw new AgentGraphInvalidInputError(`invalid agent graph ${name}`)
  }
  return value
}

function requireText(value: string, maxChars: number, name: string): string {
  if (
    typeof value !== 'string' ||
    value.length === 0 ||
    value.length > maxChars ||
    value.trim() !== value ||
    TEXT_CONTROL_CHARS.test(value)
  ) {
    throw new AgentGraphInvalidInputError(`invalid agent graph ${name}`)
  }
  return value
}

function normalizeUniqueIdentities(values: readonly string[] | undefined, name: string): string[] {
  if (values === undefined) return []
  if (values.length > MAX_INPUT_IDS) {
    throw new AgentGraphInvalidInputError(
      `agent graph ${name} exceeds ${MAX_INPUT_IDS} entries (got ${values.length})`,
    )
  }
  const normalized = values.map(value => requireIdentity(value, name)).sort(compareIdentity)
  ensureUnique(normalized, name)
  return normalized
}

function ensureUnique(values: readonly string[], name: string): void {
  if (new Set(values).size !== values.length) {
    throw new AgentGraphInvalidInputError(`agent graph repeats ${name}`)
  }
}

function compareIdentity(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0
}

function requireGraphId(value: unknown): string {
  if (typeof value !== 'string') {
    throw new AgentGraphInvalidInputError('invalid agent graph graphId')
  }
  return requireIdentity(value, 'graph id')
}

export function normalizeWorkTarget(input: AgentGraphToolAddWork): AgentGraphWorkTarget {
  if (input.targetKind === 'new_agent') {
    return { kind: 'agent', id: requireIdentity(input.agentId, 'agent id') }
  }
  if (input.targetKind === 'new_preset') {
    return { kind: 'preset', id: requireIdentity(input.subagentId, 'subagent preset id') }
  }
  if (input.targetKind === 'existing_operator') {
    return { kind: 'operator', id: requireIdentity(input.operatorId, 'operator id') }
  }
  const count =
    (input.agentId !== undefined ? 1 : 0) +
    (input.subagentId !== undefined ? 1 : 0) +
    (input.operatorId !== undefined ? 1 : 0)
  if (count !== 1) {
    throw new AgentGraphInvalidInputError(
      'exactly one of subagentId, agentId, or operatorId is required',
    )
  }
  if (input.subagentId !== undefined) {
    return { kind: 'preset', id: requireIdentity(input.subagentId, 'subagent preset id') }
  }
  if (input.agentId !== undefined) return { kind: 'agent', id: requireIdentity(input.agentId, 'agent id') }
  return { kind: 'operator', id: requireIdentity(input.operatorId, 'operator id') }
}

/** One normalized add-work entry and whether the model supplied the work id. */
export interface CompiledAgentGraphAddWork {
  readonly work: AgentGraphScheduledWork
  readonly explicitWorkId: boolean
}

/**
 * Syntactic validation of one add-work entry after cleaning, plus the
 * deterministic per-entry workId (Maka: `graph_work_<sha256(updateId, index)>`
 * unless the caller supplies `workId`).
 */
export function compileAddWork(
  input: AgentGraphToolAddWork,
  updateId: string,
  index: number,
): CompiledAgentGraphAddWork {
  const target = normalizeWorkTarget(input)
  const instruction = requireText(
    input.instruction.trim(),
    MAX_SCHEDULE_INSTRUCTION_LENGTH,
    'instruction',
  )
  const inputIds = normalizeUniqueIdentities(input.inputIds, 'input id')
  const selectedResultInputs: AgentGraphSelectedResultInput[] = (
    input.selectedResultInputs ?? []
  ).map(item => ({
    sourceGraphId: requireIdentity(item.sourceGraphId, 'source graph id'),
    resultId: requireIdentity(item.resultId, 'selected result id'),
  }))
  const selectedResultIds = selectedResultInputs.map(item => item.resultId)
  ensureUnique(selectedResultIds, 'selected result id')
  if (inputIds.length + selectedResultInputs.length > MAX_INPUT_IDS) {
    throw new AgentGraphInvalidInputError(
      `combined graph inputs must contain at most ${MAX_INPUT_IDS} entries`,
    )
  }
  for (const selected of selectedResultInputs) {
    if (inputIds.includes(selected.resultId)) {
      throw new AgentGraphInvalidInputError(
        'a result id cannot be both a current and historical graph input',
      )
    }
  }
  const explicitWorkId = input.workId !== undefined
  const workId = explicitWorkId
    ? requireIdentity(input.workId, 'work id')
    : `graph_work_${stableHash32({ schemaVersion: AGENT_GRAPH_TOOL_UPDATE_SCHEMA_VERSION, updateId, index })}`
  const replaces = input.replaces === undefined
    ? undefined
    : requireIdentity(input.replaces, 'replacement target id')
  const work: AgentGraphScheduledWork = {
    workId,
    target,
    instruction,
    inputIds,
    ...(selectedResultInputs.length > 0 ? { selectedResultInputs } : {}),
    ...(replaces !== undefined ? { replaces } : {}),
  }
  return { work, explicitWorkId }
}

/** Live-graph facts the cross-entry validation needs (projection + claims). */
export interface AgentGraphUpdateProjectionContext {
  /** Work rows of the live projection (any status). */
  readonly work: readonly { workId: string; status: AgentGraphWorkStatus }[]
  /** Intent ids already claimed (from the store claim rows). */
  readonly claimedIntentIds: ReadonlySet<string>
  /** Committed record ids (for the Maka finish-results check). */
  readonly committedRecordIds: ReadonlySet<string>
}

/**
 * Cross-entry validation against the LIVE projection: `replaces` must name an
 * existing work id (never the work being added itself), `stop` targets must
 * exist, and finish requires no pending non-terminal work that is not already
 * claimed. Quiescence is not closure: already-claimed work may still dispatch,
 * so claims do not block finish; only requested, unclaimed work does. The
 * store stays the authority — a concurrent update races the revision CAS, not
 * this read.
 */
export function validateUpdateAgainstProjection(
  input: {
    readonly addWork: readonly AgentGraphScheduledWork[]
    readonly stop: readonly AgentGraphScheduleStop[]
    readonly finish?: AgentGraphScheduleFinish
  },
  context: AgentGraphUpdateProjectionContext,
  graphId: string,
): void {
  const workIds = new Set(context.work.map(work => work.workId))
  const updateWorkIds = new Set(input.addWork.map(work => work.workId))
  for (const work of input.addWork) {
    if (work.replaces === undefined) continue
    if (work.replaces === work.workId) {
      throw new AgentGraphInvalidInputError(
        `agent graph ${graphId}: work ${work.workId} cannot replace itself`,
      )
    }
    if (updateWorkIds.has(work.replaces) || !workIds.has(work.replaces)) {
      throw new AgentGraphInvalidInputError(
        `agent graph ${graphId}: replaces target ${work.replaces} is not an existing work in the live projection`,
      )
    }
  }
  for (const stopped of input.stop) {
    if (!workIds.has(stopped.targetId)) {
      throw new AgentGraphInvalidInputError(
        `agent graph ${graphId}: stop target ${stopped.targetId} is not an existing work in the live projection`,
      )
    }
  }
  if (input.finish !== undefined) {
    const pending = context.work
      .filter(
        work =>
          work.status === 'requested' &&
          !context.claimedIntentIds.has(scheduledWorkIntentId(graphId, work.workId)),
      )
      .map(work => work.workId)
    if (pending.length > 0) {
      throw new AgentGraphInvalidInputError(
        `agent graph ${graphId}: finish requires no pending non-terminal work (pending: ${pending.join(', ')})`,
      )
    }
    const missing = input.finish.resultIds.filter(
      resultId => !context.committedRecordIds.has(resultId),
    )
    if (missing.length > 0) {
      throw new AgentGraphInvalidInputError(
        `agent graph ${graphId}: finish result ids are not committed graph records: ${missing.join(', ')}`,
      )
    }
  }
}

/**
 * Validate the model input syntactically (bounds + arity) and compile it into
 * a store-ready {@link AgentGraphScheduleUpdateRequest}. Pure with respect to
 * the store: no rows are read or written here.
 * @param input - graph id, the tool input, and the durable source triple.
 * @returns the deterministic, idempotent update request.
 */
export function compileAgentGraphScheduleUpdate(input: {
  readonly graphId: string
  readonly source: AgentGraphScheduleUpdateSource
  readonly args: UpdateAgentGraphToolInput
}): AgentGraphScheduleUpdateRequest {
  const graphId = requireGraphId(input.args.graphId)
  const raw = cleanUpdateInput({ ...input.args, graphId })
  const idempotencyKey = raw.idempotencyKey === undefined
    ? undefined
    : requireIdentity(raw.idempotencyKey, 'idempotency key')
  const source: AgentGraphScheduleUpdateSource = idempotencyKey === undefined
    ? { ...input.source, orchestrationMode: 'graph' }
    : {
      ...input.source,
      runId: idempotencyKey,
      turnId: idempotencyKey,
      toolCallId: idempotencyKey,
      orchestrationMode: 'graph',
    }
  const updateId = graphUpdateId(graphId, source)

  const addWorkInput =
    raw.operation === undefined || raw.operation === 'add_work'
      ? (raw.addWork ?? [])
      : []
  const stopInput =
    raw.operation === undefined || raw.operation === 'stop'
      ? (raw.stop ?? [])
      : []
  const finishInput =
    raw.finish !== undefined && (raw.operation === undefined || raw.operation === 'finish')
      ? raw.finish
      : undefined

  if (addWorkInput.length + stopInput.length + (finishInput !== undefined ? 1 : 0) === 0) {
    throw new AgentGraphInvalidInputError(
      'at least one addWork, stop, or finish operation is required',
    )
  }
  if (finishInput !== undefined && addWorkInput.length > 0) {
    throw new AgentGraphInvalidInputError('finish cannot be combined with addWork')
  }
  if (addWorkInput.length > MAX_ADD_WORK) {
    throw new AgentGraphInvalidInputError(
      `one graph update may add at most ${MAX_ADD_WORK} work items (got ${addWorkInput.length})`,
    )
  }
  if (stopInput.length > MAX_WORK_STOPS) {
    throw new AgentGraphInvalidInputError(
      `one graph update may stop at most ${MAX_WORK_STOPS} targets (got ${stopInput.length})`,
    )
  }
  if (finishInput !== undefined && finishInput.resultIds.length > MAX_SCHEDULE_RESULT_IDS) {
    throw new AgentGraphInvalidInputError(
      `finish may select at most ${MAX_SCHEDULE_RESULT_IDS} result ids (got ${finishInput.resultIds.length})`,
    )
  }

  let totalSelected = 0
  const addWork: AgentGraphScheduledWork[] = addWorkInput.map((rawWork, index) => {
    const compiled = compileAddWork(cleanAddWorkInput(rawWork), updateId, index)
    totalSelected += (compiled.work.selectedResultInputs ?? []).length
    return compiled.work
  })
  if (totalSelected > MAX_SELECTED_RESULT_INPUTS) {
    throw new AgentGraphInvalidInputError(
      `one graph update may select at most ${MAX_SELECTED_RESULT_INPUTS} historical results (got ${totalSelected})`,
    )
  }

  const stop: AgentGraphScheduleStop[] = stopInput
    .map(entry => ({
      targetId: requireIdentity(entry.targetId, 'stop target id'),
      reason: requireText(entry.reason.trim(), MAX_SCHEDULE_REASON_CHARS, 'stop reason'),
    }))
    .sort((a, b) => compareIdentity(a.targetId, b.targetId))
  ensureUnique(stop.map(entry => entry.targetId), 'stop target id')

  const finish = finishInput === undefined
    ? undefined
    : {
      resultIds: normalizeUniqueIdentities(finishInput.resultIds, 'finish result id'),
      reason: requireText(finishInput.reason.trim(), MAX_SCHEDULE_REASON_CHARS, 'finish reason'),
    }
  if (finish !== undefined && finish.resultIds.length === 0) {
    throw new AgentGraphInvalidInputError('finish requires at least one result id')
  }

  const semantic = {
    schemaVersion: AGENT_GRAPH_TOOL_UPDATE_SCHEMA_VERSION,
    updateId,
    graphId,
    source,
    addWork,
    stop,
    ...(finish !== undefined ? { finish } : {}),
  }
  return {
    ...semantic,
    updateFingerprint: stableHash(semantic),
  }
}
