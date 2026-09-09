/**
 * The `orchestration:graph` system-prompt section (Maka supervisor-tools
 * contract card, slice P4): what the three graph tools are for, the
 * yield-don't-poll rule, and the outcome-reporting shape of every graph
 * decision.
 * @module @hy-sde-org/dsh-tool-graph/prompt
 */

import type { PromptSection } from '@deepseek-ai/dsh-system-prompt'

/** Plugin configuration contributed by the prompt section. */
export interface GraphModePromptConfig {
  /** Disable the prompt section entirely (default false). */
  enabled?: boolean
}

export const GRAPH_MODE_PROMPT_SECTION_NAME = 'orchestration:graph'
const SECTION_ORDER = 127

/**
 * The `orchestration:graph` section text. Pinned model-visible text: the
 * supervisor announces the graph it runs, schedules durable decisions through
 * the tools, and yields at the end of a wave instead of polling.
 */
export const GRAPH_MODE_PROMPT: string = [
  'Agent Graph mode (port of Maka): you are the root supervisor of one durable agent graph. Inspect with `view_agent_graph` (bounded: work statuses, recent records, omitted counts — page with the returned nextCursor), schedule with `update_agent_graph` (addWork with exactly one of subagentId/agentId/operatorId, stop, or finish; pass idempotencyKey for retry-safe updates), and hand back control with `yield_agent_graph`.',
  'Yield, don\'t poll: after each scheduling wave with no immediate decision, end the turn with `yield_agent_graph` — the host wakes you at the next durable graph checkpoint. Do not sleep, loop on view, or emit waiting messages; `yield_agent_graph` returns `nothing_to_yield` when nothing is pending.',
  'Report outcomes, not mechanics: one block per wave — what work was added/stopped/finished, what remains pending, what is blocked, and what the graph needs. Keep work instructions concrete and bounded (60000 chars, 64 inputs); never invent work ids — read them from a view, and `replaces` only existing ones.',
].join('\n')

/**
 * Build the `orchestration:graph` prompt section.
 * @param config - section configuration.
 * @returns the {@link PromptSection} to register.
 */
export function buildGraphModePromptSection(config: GraphModePromptConfig = {}): PromptSection {
  return {
    name: GRAPH_MODE_PROMPT_SECTION_NAME,
    order: SECTION_ORDER,
    text: config.enabled === false ? '' : GRAPH_MODE_PROMPT,
  }
}
