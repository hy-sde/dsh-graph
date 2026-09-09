/**
 * Facade contracts of the Agent Graph host assembly (Maka port, slices P6-P7a).
 *
 * Every real harness service the assembly needs is narrowed to a structural
 * facade here, so the assembler and its tests never widen to full service
 * classes. The plugin maps each facade onto the cordis services it injects
 * (`subagents`, `git`, `compaction`, `sessions`, `agents`, ...).
 * @module
 */

import type { Agent } from '@deepseek-ai/dsh-agent'
import type { AgentGraphController } from '@hy-sde-org/dsh-tool-graph'
import type { GraphWakeRuntime } from '@hy-sde-org/dsh-graph-wakes'
import type { GraphControlStore } from '@hy-sde-org/dsh-graph-control'
import type { AgentGraphExecutor } from '@hy-sde-org/dsh-graph-stream'
import type { SubagentRun, SubagentStartRequest } from '@deepseek-ai/dsh-subagent'
import type { KvUnit, KvUnitDescriptor } from '@deepseek-ai/dsh-storage'
import type { SessionEventMap } from '@deepseek-ai/dsh-session/types'
import type { SessionGraphProjection } from '@hy-sde-org/dsh-graph-projection/types'

/* ------------------------------- facades ------------------------------ */

/** Storage seam: open one KV unit by descriptor (plugin maps it to `backend.kv.open`). */
export interface GraphHostStorage {
  open(descriptor: KvUnitDescriptor): Promise<KvUnit>
}

/** Subagent seam: one named-provider start (plugin maps it to `ctx.subagents.start`). */
export interface GraphHostSubagents {
  readonly provider: string
  start(name: string, request: SubagentStartRequest): Promise<SubagentRun>
}

/** One pooled worktree the host can adopt or cut (plugin maps it to the git engine pool). */
export interface GraphHostWorktreeEntry {
  readonly name: string
  readonly path: string
  readonly branch?: string
  readonly leaseHolder?: string
  readonly leased: boolean
  readonly exists: boolean
}

/** Worktree seam over the git worktree engine (pool root, holder, named branch). */
export interface GraphHostWorktrees {
  /** Repository root the pool belongs to (absolute). */
  readonly repoRoot: string
  acquire(options: {
    holder: string
    branch?: string
    signal?: AbortSignal
  }): Promise<{ readonly leaseId: string; readonly path: string; readonly repoRoot: string }>
  list(): Promise<readonly GraphHostWorktreeEntry[]>
}

/** Compaction seam: one bounded programmatic compaction of a root session. */
export interface GraphHostCompaction {
  request(sessionId: string): Promise<void>
}

/** Session-event seam: append one durable `graph/change` event to a session. */
export interface GraphHostSessionEvents {
  appendGraphChange(sessionId: string, data: SessionEventMap['graph/change']): Promise<boolean>
}

/** Idle seam: subscribe to one root session's idle boundaries; returns the unsubscriber. */
export interface GraphHostIdle {
  observe(rootSessionId: string, onIdle: (sessionId: string) => void): () => void
}

/** Clock seam (wall time in ms), injectable for deterministic tests. */
export interface GraphHostClock {
  now(): number
}

/** Services the host assembly publishes. */
export interface GraphHostServices {
  readonly store: GraphControlStore
  readonly controller: AgentGraphController
  readonly executor: AgentGraphExecutor
  readonly wakeRuntime: GraphWakeRuntime
  /** Build the bounded session projection of one graph. */
  snapshotFor(graphId: string): Promise<SessionGraphProjection>
  /** Append one `graph/change` event for a graph to a session. */
  emitGraphChange(
    sessionId: string,
    graphId: string,
    snapshot: SessionGraphProjection,
    revision: number,
  ): Promise<boolean>
  /** Attach one graph to this host's root session (idempotent). */
  attachGraph(graphId: string): Promise<void>
  /** Stop the wake runtime, cancel in-flight children, and close the store. */
  dispose(): Promise<void>
}

/** Constructor input of {@link createGraphHostServices} (see `assembler.ts`). */
export interface GraphHostAssemblerOptions {
  /** The graph's root session; only this session may drive supervisor tools. */
  readonly rootSessionId: string
  readonly storage: GraphHostStorage
  readonly subagents: GraphHostSubagents
  readonly worktrees: GraphHostWorktrees
  readonly compaction: GraphHostCompaction
  readonly sessionEvents: GraphHostSessionEvents
  readonly idle: GraphHostIdle
  /** Resolve the live parent Agent of each child run (the graph root agent). */
  readonly resolveParentAgent: (rootSessionId: string) => Agent | undefined
  readonly clock?: GraphHostClock
  readonly newId?: () => string
  /** Max new operator activations per drive (default 4). */
  readonly maxNewActivations?: number
  /** Observes assembly failures that never fail an activation (best-effort). */
  readonly onError?: (error: unknown) => void
}
