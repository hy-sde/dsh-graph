/**
 * One-shot child runner of the Agent Graph host assembly: starts each operator
 * activation as a subagent child of the graph root agent, records the run→
 * identity association the record sink needs, and maps `SubagentResult` stop
 * reasons onto the executor's child-run outcome.
 * @module
 */

import type { Agent } from '@deepseek-ai/dsh-agent'
import type { ContentBlock } from '@deepseek-ai/dsh-llm'
import type {
  AgentGraphChildRun,
  GraphOperatorChildRunner,
  GraphOperatorChildStartInput,
} from '@hy-sde-org/dsh-graph-executor'
import type { SubagentRun, SubagentStopReason } from '@deepseek-ai/dsh-subagent'
import type { GraphHostSubagents } from './types.ts'
import { GraphRunIdentityLedger } from './records.ts'

interface GraphHostChildRunnerDeps {
  readonly subagents: GraphHostSubagents
  /** Resolve the live parent agent of every child run (the graph root agent). */
  readonly resolveParentAgent: (rootSessionId: string) => Agent | undefined
  readonly rootSessionId: string
  readonly ledger: GraphRunIdentityLedger
}

/**
 * Starts one subagent child per activation with the provisioned worktree as
 * the child workspace. The run→identity ledger is filled right after the start
 * so the sink can attribute the terminal event (the executor copies
 * `claim.targetRunId` into both the start input and the emitted event).
 */
export class GraphHostChildRunner implements GraphOperatorChildRunner {
  private readonly runs = new Map<string, SubagentRun>()

  constructor(private readonly deps: GraphHostChildRunnerDeps) {}

  async start(input: GraphOperatorChildStartInput): Promise<AgentGraphChildRun> {
    const parent = this.deps.resolveParentAgent(this.deps.rootSessionId)
    if (parent === undefined) {
      throw new Error(
        `agent graph host: no live root agent for ${this.deps.rootSessionId}; cannot start operator child ${input.runId}`,
      )
    }
    const operatorId = input.labels?.operatorId ?? ''
    const run = await this.deps.subagents.start(`graph-operator:${operatorId}`, {
      label: `graph-operator:${operatorId}`,
      prompt: [{ type: 'text', text: input.instructions }],
      parent,
      signal: input.abortSignal ?? new AbortController().signal,
      ...(input.workspace !== undefined ? { workspace: input.workspace } : {}),
    })
    this.runs.set(input.sessionId, run)
    if (input.runId !== undefined) {
      this.deps.ledger.record(input.runId, {
        graphId: input.labels?.graphId ?? '',
        operatorId,
        sessionId: input.sessionId,
      })
    }
    try {
      const result = await run.result
      const summary = summaryOf(result.output)
      return {
        outcome: outcomeOf(result.stopReason),
        ...(summary !== undefined ? { summary } : {}),
      }
    } catch (error) {
      return { outcome: 'failed', error }
    }
  }

  async stop(sessionId: string, opts?: { reason?: string }): Promise<void> {
    void opts
    const run = this.runs.get(sessionId)
    if (run === undefined) return
    await run.dispose()
    this.runs.delete(sessionId)
  }

  /** Cancel and dispose every in-flight child (best-effort; used by `dispose`). */
  async stopAll(): Promise<void> {
    const runs = [...this.runs.values()]
    this.runs.clear()
    await Promise.allSettled(runs.map(run => run.dispose()))
  }
}

function outcomeOf(stopReason: SubagentStopReason): AgentGraphChildRun['outcome'] {
  switch (stopReason) {
    case 'completed':
      return 'fulfilled'
    case 'aborted':
      return 'cancelled'
    case 'error':
    case 'max-tokens':
    case 'refusal':
      return 'failed'
    default:
      // Stop reasons widen as backends merge in variants; treat unknowns as a
      // failed activation rather than silently fulfilling it.
      return 'failed'
  }
}

function summaryOf(output: readonly ContentBlock[]): string | undefined {
  let text = ''
  for (const block of output) {
    if (block.type === 'text') text += block.text
  }
  return text.length > 0 ? text : undefined
}
