/**
 * Cordis function plugin mounting the Agent Graph host assembly for the graph
 * root agent: builds the facades over the real services (storage backend,
 * subagents, git worktree engine, compaction, live sessions, idle status) and
 * provides the `agentGraphController` service the supervisor tools consume.
 * @module
 */

import type { Agent } from '@deepseek-ai/dsh-agent'
import type { CompactionEngine } from '@deepseek-ai/dsh-compaction'
import type { Context } from '@deepseek-ai/cordis'
import {
  acquireWorktree,
  listWorktrees,
  primaryRepoRoot,
  type GitService,
  type WorktreeSettings,
} from '@hy-sde-org/dsh-git'
import {
  SessionId,
  type Session,
} from '@deepseek-ai/dsh-session'
import { storageBackendServiceKey, type StorageBackend } from '@deepseek-ai/dsh-storage'
import { AGENT_GRAPH_CONTROLLER_SERVICE } from '@hy-sde-org/dsh-tool-graph'
import z from '@deepseek-ai/schemastery'
import { createGraphHostServices } from './assembler.ts'
import type { SessionEventMap } from '@deepseek-ai/dsh-session/types'
import type {
  GraphHostCompaction,
  GraphHostIdle,
  GraphHostServices,
  GraphHostSessionEvents,
  GraphHostStorage,
  GraphHostSubagents,
  GraphHostWorktrees,
} from './types.ts'

/** Name of the published controller service (equal to the tool-graph constant). */
export const SERVICE_AGENT_GRAPH_CONTROLLER = AGENT_GRAPH_CONTROLLER_SERVICE

/** Additional service provided by this plugin: the whole assembly handle. */
export const SERVICE_GRAPH_HOST = 'graphHostServices'

/** Cordis plugin name. */
export const name = 'graph-host'
/** Services the facade construction reads; the controller service is published. */
export const inject = ['agents', 'sessions', 'subagents', 'git', 'compaction']

/**
 * Plugin configuration. `rootSessionId` names the graph root — the session
 * whose agent is the parent of every operator child and the owner of the
 * `graph/change` events. `subagentProvider` names the provider `subagents.start`
 * forwards to (one-shot in-process spawn for the shipped assembly).
 */
export interface Config {
  /** Graph root session id (must exist as a live root agent when the assembly builds). */
  rootSessionId: string
  /** Name of the subagent provider every operator child is started with. */
  subagentProvider: string
  /** Storage backend name whose `kv` facet hosts the graph control unit (default `sqlite`). */
  backend?: string
  /** Worktree pool repository root; defaults to the process working directory's primary repo root. */
  worktreeRepoRoot?: string
  /** Branch operator worktrees are cut from (default: the pool's inferred base branch). */
  worktreeBaseBranch?: string
  /** Cap on pooled worktree slots for the repository (default: unlimited). */
  worktreeMaxSlots?: number
  /** Max new operator activations per reconcile drive (default 4). */
  maxNewActivations?: number
}

/** Plugin config schema; `rootSessionId` must exist at apply time. */
export const Config: z<Config> = z.object({
  rootSessionId: z.string().required(),
  subagentProvider: z.string().required(),
  backend: z.string().default('sqlite'),
  worktreeRepoRoot: z.string(),
  worktreeBaseBranch: z.string(),
  worktreeMaxSlots: z.number().step(1).min(1),
  maxNewActivations: z.number().step(1).min(1),
})

/**
 * Mount the graph host for the configured root session. The assembly builds
 * once, when the root agent publishes (or when it is already live); the
 * controller services are provided on this plugin's fiber so an unload
 * withdraws them with it.
 * @param ctx - plugin context carrying the injected services.
 * @param config - validated plugin configuration.
 */
export function apply(ctx: Context, config: Config): void {
  const state = new PluginState(ctx, config)
  ctx.effect(() => {
    const stopCreated = ctx.on('agent/created', ({ agent }) => {
      if (String(agent.id) !== config.rootSessionId) return
      state.attachIdleListener(agent)
      void state.build()
    })
    const root = ctx.agents.get(SessionId(config.rootSessionId))
    if (root !== undefined) {
      state.attachIdleListener(root)
      void state.build()
    }
    return async () => {
      stopCreated()
      await state.dispose()
    }
  }, 'graphHost.install()')
}

/** Per-plugin mutable state; one instance per `apply` so HMR disposal drops it. */
class PluginState {
  private building = false
  private services: GraphHostServices | undefined
  private readonly idleCallbacks = new Map<string, Set<(sessionId: string) => void>>()
  private readonly idleListeners = new Map<string, () => void>()

  constructor(
    private readonly ctx: Context,
    private readonly config: Config,
  ) { }

  /** Build the assembly once and publish the services; a failed build fails loud. */
  async build(): Promise<void> {
    if (this.building || this.services !== undefined) return
    this.building = true
    try {
      const services = await createGraphHostServices({
        rootSessionId: this.config.rootSessionId,
        storage: storageFacade(this.ctx, this.config),
        subagents: subagentsFacade(this.ctx, this.config),
        worktrees: await worktreesFacade(this.ctx, this.config),
        compaction: compactionFacade(this.ctx),
        sessionEvents: sessionEventsFacade(this.ctx),
        idle: idleFacade(this),
        resolveParentAgent: sessionId => this.ctx.agents.get(SessionId(sessionId)),
        ...(this.config.maxNewActivations !== undefined
          ? { maxNewActivations: this.config.maxNewActivations }
          : {}),
      })
      this.services = services
      this.ctx.provide(SERVICE_AGENT_GRAPH_CONTROLLER, services.controller)
      this.ctx.provide(SERVICE_GRAPH_HOST, services)
    } finally {
      this.building = false
    }
  }

  /** Subscribe one root agent's `agent/status` stream; the subscription unloads with the agent. */
  attachIdleListener(agent: Agent): void {
    if (this.idleListeners.has(String(agent.id))) return
    const id = String(agent.id)
    const stopStatus = agent.ctx.on('agent/status', ({ status }) => {
      if (status === 'idle') this.reportIdle(id)
    })
    this.idleListeners.set(id, stopStatus)
    agent.ctx.effect(() => stopStatus, 'graphHost.idleListener()')
  }

  /** Record one idle subscriber for a root session. */
  observeIdle(
    rootSessionId: string,
    onIdle: (sessionId: string) => void,
  ): () => void {
    const callbacks = this.idleCallbacks.get(rootSessionId) ?? new Set()
    callbacks.add(onIdle)
    this.idleCallbacks.set(rootSessionId, callbacks)
    return () => {
      callbacks.delete(onIdle)
    }
  }

  /** Forward one idle boundary to every subscriber of the session. */
  reportIdle(sessionId: string): void {
    for (const callback of [...(this.idleCallbacks.get(sessionId) ?? [])]) {
      callback(sessionId)
    }
  }

  /** Unsubscribe every status listener and await the assembly's disposal. */
  async dispose(): Promise<void> {
    for (const stop of this.idleListeners.values()) stop()
    this.idleListeners.clear()
    this.idleCallbacks.clear()
    const services = this.services
    this.services = undefined
    if (services !== undefined) await services.dispose()
  }
}

function idleFacade(state: PluginState): GraphHostIdle {
  return {
    observe: (rootSessionId, onIdle) => state.observeIdle(rootSessionId, onIdle),
  }
}

function storageFacade(ctx: Context, config: Config): GraphHostStorage {
  const backendName = config.backend ?? 'sqlite'
  const backend = ctx.get(
    storageBackendServiceKey(backendName),
  ) as StorageBackend | undefined
  if (backend === undefined || backend.kv === undefined) {
    throw new Error(
      `graph-host: storage backend '${backendName}' is not registered (or exposes no kv facet) — load its backend plugin (storage.backend.<name>) before graph-host`,
    )
  }
  const kv = backend.kv
  return {
    open: descriptor => kv.open(descriptor),
  }
}

function subagentsFacade(ctx: Context, config: Config): GraphHostSubagents {
  return {
    provider: config.subagentProvider,
    start: (internalName, request) => ctx.subagents.start(internalName, request),
  }
}

async function worktreesFacade(ctx: Context, config: Config): Promise<GraphHostWorktrees> {
  const cwd = config.worktreeRepoRoot ?? process.cwd()
  const repoRoot = await primaryRepoRoot(ctx.git, cwd)
  const settings: WorktreeSettings = {
    ...(config.worktreeBaseBranch !== undefined
      ? { baseBranch: config.worktreeBaseBranch }
      : {}),
    ...(config.worktreeMaxSlots !== undefined
      ? { maxSlots: config.worktreeMaxSlots }
      : {}),
  }
  const git: GitService = ctx.git
  return {
    repoRoot,
    async acquire(options) {
      const lease = await acquireWorktree(git, cwd, settings, {
        holder: options.holder,
        ...(options.branch !== undefined ? { branch: options.branch } : {}),
        ...(options.signal !== undefined ? { signal: options.signal } : {}),
      })
      return {
        leaseId: lease.leaseId,
        path: lease.path,
        repoRoot: await primaryRepoRoot(git, cwd),
      }
    },
    async list() {
      const entries = await listWorktrees(git, cwd, { settings })
      return entries.map(entry => ({
        name: entry.name,
        path: entry.path,
        ...(entry.branch !== undefined ? { branch: entry.branch } : {}),
        ...(entry.leaseHolder !== undefined
          ? { leaseHolder: entry.leaseHolder }
          : {}),
        leased: entry.leased,
        exists: entry.exists,
      }))
    },
  }
}

function compactionFacade(ctx: Context): GraphHostCompaction {
  return {
    request: async (sessionId: string): Promise<void> => {
      const agent = ctx.agents.get(SessionId(sessionId))
      if (agent === undefined) {
        throw new Error(`graph-host: compaction requested for a session with no live agent (${sessionId})`)
      }
      const engine: CompactionEngine = ctx.compaction
      await agent.runMaintenance(signal =>
        engine.compactNow(agent, signal).then(() => undefined),
      )
    },
  }
}

function sessionEventsFacade(ctx: Context): GraphHostSessionEvents {
  return {
    appendGraphChange: (
      sessionId: string,
      data: SessionEventMap['graph/change'],
    ): Promise<boolean> => {
      const session: Session | undefined = ctx.sessions.get(SessionId(sessionId))
      if (session === undefined) return Promise.resolve(false)
      session.append('graph/change', data)
      return Promise.resolve(true)
    },
  }
}
