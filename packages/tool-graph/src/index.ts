/**
 * Agent Graph supervisor tools over a host-provided graph controller (Maka
 * port, slice P4): `view_agent_graph`, `update_agent_graph`,
 * `yield_agent_graph`, the `orchestration:graph` prompt section, and the
 * controller + snapshot machinery the tools run on. The package contributes
 * no graph state of its own — the composition constructs one
 * {@link AgentGraphController} and provides it under
 * {@link AGENT_GRAPH_CONTROLLER_SERVICE}; this plugin consumes it.
 *
 * Agent-plane: this package mounts as a preset row and resolves the host
 * `agentGraphController` service with `ctx.get` (optional service: without the
 * controller the tools still mount, so a session never fails to create just
 * because the graph host is absent or its root session is stale — and every
 * graph tool call fails loud with `[agent-graph-unavailable]` until the host
 * provides it). The call-time failure keeps the port's "no silent no-op"
 * intent without letting an optional host assembly block the whole preset.
 * Root-only enforcement: the controller records `rootSessionId`, and every
 * tool derives the calling session from the live execution context and
 * rejects a non-root caller before it touches durable state — DSH has no
 * `nesting: 'direct_only'` tool flag, so this is the closest available check.
 * @module @hy-sde-org/dsh-tool-graph
 */

import { Context } from '@deepseek-ai/cordis'
import type { } from '@deepseek-ai/dsh-agent'
import type { } from '@deepseek-ai/dsh-session-projection'
import {
  ToolArgsError,
  parameterSchemaSpecToJsonSchema,
  validateArgs,
  valueSchemaSpecToJsonSchema,
  type ToolRunContext,
} from '@deepseek-ai/dsh-tools'
import { AgentGraphToolError } from './errors.ts'
import type { AgentGraphController } from './controller.ts'
import { registerAgentGraphTools, type ToolCallIdentity } from './tools.ts'
import { buildGraphModePromptSection, type GraphModePromptConfig } from './prompt.ts'

export { AgentGraphController, createAgentGraphController, AGENT_GRAPH_CONTROLLER_SERVICE } from './controller.ts'
export type {
  AgentGraphControllerOptions,
  AgentGraphControllerCoordinatorOptions,
  AgentGraphControllerSnapshot,
  AgentGraphControllerScheduleResult,
  AgentGraphControllerYieldResult,
} from './controller.ts'
export { registerAgentGraphTools } from './tools.ts'
export type {
  ToolCallIdentity,
  AgentGraphToolDefinition,
  AgentGraphToolDeps,
  ViewAgentGraphToolArgs,
  UpdateAgentGraphToolArgs,
  YieldAgentGraphToolArgs,
  UpdateAgentGraphToolValue,
  YieldAgentGraphToolValue,
} from './tools.ts'
export { buildGraphModePromptSection, GRAPH_MODE_PROMPT, GRAPH_MODE_PROMPT_SECTION_NAME } from './prompt.ts'
export type { GraphModePromptConfig } from './prompt.ts'
export {
  compileAgentGraphScheduleUpdate,
  validateUpdateAgainstProjection,
  cleanAddWorkInput,
  cleanUpdateInput,
  AGENT_GRAPH_TOOL_UPDATE_SCHEMA_VERSION,
} from './compile.ts'
export type {
  UpdateAgentGraphToolInput,
  AgentGraphToolAddWork,
  AgentGraphToolStop,
  AgentGraphToolFinish,
  AgentGraphToolOperation,
  AgentGraphToolTargetKind,
  AgentGraphUpdateProjectionContext,
  CompiledAgentGraphAddWork,
} from './compile.ts'
export {
  buildAgentGraphToolSnapshot,
  TOOL_VIEW_MAX_TERMINAL_WORK,
  TOOL_VIEW_MAX_STOPPED_TARGETS,
  TOOL_VIEW_MAX_INSTRUCTION_CHARS,
  TOOL_VIEW_MAX_RECORDS,
  TOOL_VIEW_MAX_SUMMARY_CHARS,
  TOOL_VIEW_MAX_READINESS,
  TOOL_VIEW_MAX_LIVE_STATE,
} from './view.ts'
export type { AgentGraphToolSnapshot, AgentGraphToolWorkView, AgentGraphToolRecordView, AgentGraphToolIntentView } from './view.ts'
export {
  AgentGraphToolError,
  AgentGraphNotRootSessionError,
  AgentGraphInvalidInputError,
  AgentGraphUnknownGraphError,
  AgentGraphNothingToYieldError,
  AgentGraphClosedError,
} from './errors.ts'
export type { AgentGraphToolErrorCode } from './errors.ts'

/** Plugin configuration. */
export interface Config extends GraphModePromptConfig {
  /** Disable the tool registrations and prompt section entirely (default false). */
  enabled?: boolean
}

/** Cordis plugin name for loader diagnostics. */
export const name = 'tool-graph'

/** Services consumed by this plugin (the graph controller is resolved opportunistically with `ctx.get`). */
export const inject = ['tools', 'systemPrompt', 'sessionProjections']

/**
 * Register the three supervisor tools and the `orchestration:graph` prompt
 * section over the host-provided controller. Without the controller the tools
 * still register but every call fails loud with `[agent-graph-unavailable]` —
 * mounting a preset must never break session creation over an optional host.
 * @param ctx - the agent-plane plugin context (injects `tools`, `systemPrompt`, `sessionProjections`).
 * @param config - resolved plugin configuration.
 */
export function apply(ctx: Context, config: Config = {}): void {
  if (config.enabled === false) return
  const controller = ctx.get('agentGraphController')
  const unavailable = controller === undefined
    ? 'the agentGraphController service is not provided — mount graph-host with a live rootSessionId, or construct one with createAgentGraphController and provide it under AGENT_GRAPH_CONTROLLER_SERVICE'
    : undefined
  for (const def of registerAgentGraphTools({ controller: controller as AgentGraphController })) {
    const parameters = parameterSchemaSpecToJsonSchema(def.parameters)
    const outputSchema = valueSchemaSpecToJsonSchema(def.outputSchema)
    ctx.tools.register({
      name: def.name,
      description: def.description,
      parameters: parameters as unknown as Record<string, unknown>,
      output: {
        schema: outputSchema,
        render: (_args, value) => [{ type: 'text', text: def.render(_args as never, value as never) }],
      },
      execute: (args, exec) => {
        if (unavailable !== undefined) {
          throw new Error(`[agent-graph-unavailable] ${unavailable}`)
        }
        const violations = validateArgs(def.parameters, args)
        if (violations.length > 0) throw new ToolArgsError(violations)
        return def.execute(args as never, callIdentityOf(exec, ctx)).catch((error: unknown) => {
          if (error instanceof AgentGraphToolError) {
            throw new Error(`[${error.code}] ${error.message}`)
          }
          throw error
        })
      },
    })
  }
  ctx.systemPrompt.section(buildGraphModePromptSection(config))
}

export default { name, inject, apply }

/** Derive the durable tool-call identity from the live execution context. */
function callIdentityOf(exec: ToolRunContext, ctx: Context): ToolCallIdentity {
  const agent = exec.agent
  if (agent === undefined) {
    throw new AgentGraphToolError(
      'configuration',
      'agent graph tools require an agent-bound caller',
    )
  }
  const lastTurn = ctx.sessionProjections.stateOf(agent.session, 'turnBoundary')?.lastTurn ?? 0
  return {
    sessionId: String(agent.session.id),
    runId: `graph_run_${String(lastTurn)}`,
    turnId: `graph_turn_${String(lastTurn)}`,
    toolCallId: String(exec.callId),
  }
}
