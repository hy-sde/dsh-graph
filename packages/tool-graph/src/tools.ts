/**
 * The three root-only, direct-only Agent Graph supervisor tools (Maka
 * `buildAgentGraphSupervisorTools` port, slice P4): `view_agent_graph`
 * (bounded read), `update_agent_graph` (durable schedule intent with Maka
 * bounds/preprocessors and source-triple idempotency), and
 * `yield_agent_graph` (cooperative turn end while graph work continues).
 *
 * The definitions are host-agnostic: `registerAgentGraphTools(deps)` returns
 * plain definitions whose `execute` takes a {@link ToolCallIdentity}; the
 * Cordis adapter in `index.ts` wires the live execution context. Root-only
 * enforcement lives here (the controller records `rootSessionId`), so a
 * non-root caller is rejected before it reaches any durable state.
 * @module
 */

import type {
  AgentGraphScheduleUpdateSource,
} from '@hy-sde-org/dsh-graph-control'
import type { ValueSchemaSpec, ParameterSchemaSpec } from '@deepseek-ai/dsh-tools'
import type { AgentGraphToolSnapshot } from './view.ts'
import { buildAgentGraphToolSnapshot } from './view.ts'
import type { AgentGraphController } from './controller.ts'
import { compileAgentGraphScheduleUpdate, validateUpdateAgainstProjection } from './compile.ts'
import type { AgentGraphToolFinish, AgentGraphToolOperation, AgentGraphToolStop, UpdateAgentGraphToolInput } from './compile.ts'
import { AgentGraphNotRootSessionError } from './errors.ts'

/** Durable identity of one supervisor tool call (the source triple inputs). */
export interface ToolCallIdentity {
  readonly sessionId: string
  readonly runId?: string
  readonly turnId: string
  readonly toolCallId: string
}

/** Dependency surface for {@link registerAgentGraphTools}. */
export interface AgentGraphToolDeps {
  readonly controller: AgentGraphController
  /** Maps the call identity to a schedule source (default: DSH turn-folded run/turn). */
  readonly resolveSource?: (call: ToolCallIdentity) => AgentGraphScheduleUpdateSource
}

/** One host-agnostic tool definition. */
export interface AgentGraphToolDefinition<A, V> {
  readonly name: string
  readonly description: string
  readonly parameters: ParameterSchemaSpec
  readonly outputSchema: ValueSchemaSpec
  execute(args: A, call: ToolCallIdentity): Promise<V>
  render(args: A, value: V): string
}

export interface ViewAgentGraphToolArgs {
  readonly graphId: string
  readonly cursor?: string
}

export type UpdateAgentGraphToolArgs = UpdateAgentGraphToolInput

export interface YieldAgentGraphToolArgs {
  readonly graphId: string
  readonly reason?: string
}

export interface UpdateAgentGraphToolValue {
  readonly update: { readonly revision: number; readonly committedAt: number; readonly created: boolean }
  readonly graph: AgentGraphToolSnapshot
}

export interface YieldAgentGraphToolValue {
  readonly deliveredOnIdle: true
  readonly wakeId: string
  readonly pendingWorkCount: number
}

/**
 * Build the three supervisor tool definitions over one controller.
 * @param deps - the controller plus the (optional) source resolver.
 * @returns `[view_agent_graph, update_agent_graph, yield_agent_graph]` in that order.
 */
export function registerAgentGraphTools(
  deps: AgentGraphToolDeps,
): readonly [
  AgentGraphToolDefinition<ViewAgentGraphToolArgs, AgentGraphToolSnapshot>,
  AgentGraphToolDefinition<UpdateAgentGraphToolArgs, UpdateAgentGraphToolValue>,
  AgentGraphToolDefinition<YieldAgentGraphToolArgs, YieldAgentGraphToolValue>,
] {
  const resolveSource = deps.resolveSource ?? defaultSource
  const controller = deps.controller

  const view: AgentGraphToolDefinition<ViewAgentGraphToolArgs, AgentGraphToolSnapshot> = {
    name: 'view_agent_graph',
    description:
      'Inspect one agent graph durably: scheduled work with statuses, bounded record summaries, readiness intents, and omitted counts. '
      + 'Pass no cursor for the current view; pass a nextCursor returned by an earlier view to page live state. Read-only.',
    parameters: {
      graphId: { type: 'string', required: true, description: 'The agent graph id to inspect.' },
      cursor: { type: 'string', description: 'Opaque page cursor from a previous view (omit for the latest view).' },
    },
    outputSchema: { type: 'json' },
    async execute(args, call) {
      assertRootSession(controller, call, args.graphId)
      const snapshot = await controller.snapshot(args.graphId)
      return buildAgentGraphToolSnapshot({
        projection: snapshot.projection,
        records: snapshot.records,
        omittedPartialCount: snapshot.omittedPartialCount,
        readiness: snapshot.readiness,
        ...(args.cursor !== undefined ? { cursor: args.cursor } : {}),
      })
    },
    render: (_args, value) => renderSnapshot(value),
  }

  const update: AgentGraphToolDefinition<UpdateAgentGraphToolArgs, UpdateAgentGraphToolValue> = {
    name: 'update_agent_graph',
    description:
      'Adjust one agent graph durably: add work, stop work, or finish it. Always set operation when a provider-filled payload '
      + 'could carry unrelated fields. addWork entries: exactly one of subagentId (new preset), agentId (legacy agent), or '
      + 'operatorId (existing operator); instruction is required (cleaned of surrounding whitespace). Limits: 32 work items, '
      + '64 input ids per update, 60000 instruction chars. replaces must name an existing work id from a previous view — it '
      + 'never replaces work added by the same update. finish requires no pending non-terminal work and committed result ids. '
      + 'Pass idempotencyKey to make a retried identical update dedupe at the store.',
    parameters: {
      graphId: { type: 'string', required: true, description: 'The agent graph id to update.' },
      operation: {
        type: 'string',
        enum: ['add_work', 'stop', 'finish'],
        description: 'Explicit operation discriminator; unrelated provider-filled payloads are ignored.',
      },
      addWork: {
        type: 'array',
        description: 'Schedule work (up to 32 items).',
        items: {
          type: 'object',
          additionalProperties: true,
          properties: {
            targetKind: {
              type: 'string',
              enum: ['new_agent', 'new_preset', 'existing_operator'],
              description: 'Explicit target discriminator; unrelated identity fields are ignored.',
            },
            agentId: { type: 'string', description: 'Legacy built-in agent id for new graph work.' },
            subagentId: { type: 'string', description: 'User-approved subagent preset id for new graph work.' },
            operatorId: { type: 'string', description: 'Runtime id of an EXISTING graph operator.' },
            instruction: { type: 'string', required: true },
            inputIds: { type: 'array', items: { type: 'string' }, description: 'Durable record ids forming this work item input frontier.' },
            selectedResultInputs: {
              type: 'array',
              items: {
                type: 'object',
                additionalProperties: false,
                properties: {
                  sourceGraphId: { type: 'string', required: true },
                  resultId: { type: 'string', required: true },
                },
              },
            },
            replaces: { type: 'string', description: 'Existing work superseded by this work item.' },
            replacementMode: { type: 'string', enum: ['none', 'replace'], description: 'none drops a provider-filled replaces.' },
            workId: { type: 'string', description: 'Optional explicit work id (normally derived deterministically).' },
          },
        },
      },
      stop: {
        type: 'array',
        items: {
          type: 'object',
          additionalProperties: false,
          properties: {
            targetId: { type: 'string', required: true },
            reason: { type: 'string', required: true },
          },
        },
      },
      finish: {
        type: 'object',
        additionalProperties: false,
        properties: {
          resultIds: { type: 'array', items: { type: 'string' }, description: 'Committed graph record ids selected as the final result.' },
          reason: { type: 'string', required: true },
        },
      },
      idempotencyKey: { type: 'string', description: 'Stable key folded into the source triple: a retried identical update with the same key is not re-committed.' },
    },
    outputSchema: { type: 'json' },
    async execute(args, call) {
      assertRootSession(controller, call, args.graphId)
      const request = compileAgentGraphScheduleUpdate({
        graphId: args.graphId,
        source: resolveSource(call),
        args,
      })
      const snapshot = await controller.snapshot(args.graphId)
      const claimedIntentIds = new Set(snapshot.claims.map(claim => claim.intentId))
      const committedRecordIds = new Set(snapshot.records.map(record => record.recordId))
      validateUpdateAgainstProjection(
        {
          addWork: request.addWork,
          stop: request.stop,
          ...(request.finish !== undefined ? { finish: request.finish } : {}),
        },
        {
          work: snapshot.projection.work,
          claimedIntentIds,
          committedRecordIds,
        },
        args.graphId,
      )
      const result = await controller.schedule(args.graphId, request)
      const finalSnapshot = await controller.snapshot(args.graphId)
      return {
        update: result.update,
        graph: buildAgentGraphToolSnapshot({
          projection: finalSnapshot.projection,
          records: finalSnapshot.records,
          omittedPartialCount: finalSnapshot.omittedPartialCount,
          readiness: finalSnapshot.readiness,
        }),
      }
    },
    render: (_args, value) => (
      `Agent graph update ${value.update.created ? 'committed' : 'reused'} at revision ${value.update.revision}.\n\n`
      + renderSnapshot(value.graph)
    ),
  }

  const yieldTool: AgentGraphToolDefinition<YieldAgentGraphToolArgs, YieldAgentGraphToolValue> = {
    name: 'yield_agent_graph',
    description:
      'End this supervisor turn successfully while scheduled graph work continues. Call after the current scheduling wave '
      + 'has no immediate decision; do not poll, sleep, or emit a waiting message. The host starts a new supervisor turn at '
      + 'the next durable graph checkpoint. This does not finish or close the graph.',
    parameters: {
      graphId: { type: 'string', required: true, description: 'The agent graph id to yield for.' },
      reason: { type: 'string', description: 'Why the supervisor has no immediate decision until the graph changes.' },
    },
    outputSchema: { type: 'json' },
    async execute(args, call) {
      assertRootSession(controller, call, args.graphId)
      const result = await controller.yield(args.graphId)
      const snapshot = await controller.snapshot(args.graphId)
      const pendingWorkCount = snapshot.projection.work.filter(
        work => work.status === 'requested',
      ).length
      return { deliveredOnIdle: true, wakeId: result.wakeId, pendingWorkCount }
    },
    render: (args, value) => (
      `Yielded agent graph ${args.graphId} (wake ${value.wakeId}); ${value.pendingWorkCount} pending items continue.`
    ),
  }

  return [view, update, yieldTool]
}

export type { AgentGraphToolOperation, AgentGraphToolStop, AgentGraphToolFinish }

function assertRootSession(controller: AgentGraphController, call: ToolCallIdentity, graphId: string): void {
  if (call.sessionId !== controller.rootSessionId) {
    throw new AgentGraphNotRootSessionError(graphId, controller.rootSessionId, call.sessionId)
  }
}

function defaultSource(call: ToolCallIdentity): AgentGraphScheduleUpdateSource {
  return {
    sessionId: call.sessionId,
    runId: call.runId ?? `graph_run_${call.turnId}`,
    turnId: call.turnId,
    toolCallId: call.toolCallId,
    orchestrationMode: 'graph',
  }
}

function renderSnapshot(snapshot: AgentGraphToolSnapshot): string {
  const lines = [
    `Agent graph ${snapshot.graphId} — ${snapshot.closed ? 'closed' : 'open'}, revision ${snapshot.revision}, ${snapshot.updateCount} update(s).`,
    `Work (${snapshot.work.length} visible):`,
  ]
  for (const work of snapshot.work) {
    const replaces = work.replaces === undefined ? '' : ` (replaces ${work.replaces})`
    lines.push(
      `  [${work.status}] ${work.workId} → ${work.target.kind}:${work.target.id}${replaces}: ${oneLine(work.instruction)}`,
    )
  }
  if (snapshot.work.length === 0) lines.push('  none')
  lines.push(`Records (${snapshot.records.length} visible):`)
  for (const record of snapshot.records) {
    lines.push(`  ${record.recordId} ${record.operatorId}: ${oneLine(record.summary)}`)
  }
  if (snapshot.records.length === 0) lines.push('  none')
  lines.push(
    `Omitted: work=${snapshot.omitted.work}, stoppedTargets=${snapshot.omitted.stoppedTargets}, `
    + `records=${snapshot.omitted.records}, partialRecords=${snapshot.omitted.partialRecords}, readiness=${snapshot.omitted.readiness}`,
  )
  if (snapshot.nextCursor !== undefined) lines.push(`Next cursor: ${snapshot.nextCursor}`)
  return lines.join('\n')
}

function oneLine(text: string): string {
  return text.replace(/\s+/g, ' ').trim().slice(0, 120)
}
