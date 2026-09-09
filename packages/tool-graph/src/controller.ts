/**
 * Host-side Agent Graph controller: one process-local {@link AgentGraphCoordinator}
 * per graphId plus the read/write surface the supervisor tools need
 * (Maka `AgentGraphSupervisorTools` host wiring, slice P4).
 *
 * The store is the authority for every decision; this class only composes
 * derivations, drives the coordinator, and writes supervisor-wake rows. The
 * root-only constraint is enforced here: the controller records the graph
 * root session id at construction, and every tool verifies the calling
 * session before it touches durable state or the wake row.
 * @module
 */

import {
  type AgentGraphScheduleStop,
  type AgentGraphScheduleUpdateRequest,
  type AgentGraphScheduleUpdateSource,
  type GraphControlStore,
  graphUpdateId,
  graphWakeId,
  type AgentGraphIntentClaimRecord,
} from '@hy-sde-org/dsh-graph-control'
import {
  AgentGraphCoordinator,
  type AgentGraphCoordinatorOptions,
  type AgentGraphRecord,
  type AgentGraphReadinessSnapshotResult,
  type AgentGraphScheduleProjection,
  type AgentGraphSupervisorObservation,
  type AgentGraphReconciliationTopology,
  buildAgentGraphReadinessSnapshot,
  composeProvisionedTopology,
  projectAgentGraphSchedule,
  readCommittedAgentGraphProjection,
  stableHash,
} from '@hy-sde-org/dsh-graph-stream'
import {
  AGENT_GRAPH_TOOL_UPDATE_SCHEMA_VERSION,
} from './compile.ts'
import {
  AgentGraphClosedError,
  AgentGraphInvalidInputError,
  AgentGraphNothingToYieldError,
  AgentGraphToolError,
} from './errors.ts'

export const AGENT_GRAPH_CONTROLLER_SERVICE = 'agentGraphController'
export const AGENT_GRAPH_CONTROLLER_SNAPSHOT_SCHEMA_VERSION = 1 as const

declare module '@deepseek-ai/cordis' {
  interface Context {
    /**
     * Optional host-provided Agent Graph controller. The tool plugin consumes
     * it with `ctx.get` (never a declared injection, so a host without one
     * fails loud at the tool plugin's load instead of at composition time).
     */
    agentGraphController?: AgentGraphController
  }
}

/** Coordinator runtime selections the controller forwards on `getOrCreate`. */
export interface AgentGraphControllerCoordinatorOptions extends Pick<
  AgentGraphCoordinatorOptions,
  | 'executor'
  | 'recordSource'
  | 'newId'
  | 'maxNewActivations'
  | 'resolveSelectedResultInputs'
  | 'hydrateInputHandoffs'
  | 'renderPrompt'
> {}

/** Options for {@link AgentGraphController}. */
export interface AgentGraphControllerOptions {
  readonly store: GraphControlStore
  /** The graph root session; only this session may call the supervisor tools. */
  readonly rootSessionId: string
  /** Id allocator forwarded to coordinators (admission identities). */
  readonly newId: () => string
  /** Host observation seam (committed records + operator terminal state). */
  readonly observeGraph?: (
    topology: AgentGraphReconciliationTopology,
  ) => Promise<AgentGraphSupervisorObservation>
  /** Coordinator runtime selections; `executor`/`recordSource`/`maxNewActivations` are required for `schedule`/`stop`. */
  readonly options?: AgentGraphControllerCoordinatorOptions
}

/** Whole-graph derivation snapshot returned by {@link AgentGraphController.snapshot}. */
export interface AgentGraphControllerSnapshot {
  readonly projection: AgentGraphScheduleProjection
  readonly records: readonly AgentGraphRecord[]
  readonly omittedPartialCount: number
  readonly readiness: AgentGraphReadinessSnapshotResult
  readonly claims: readonly AgentGraphIntentClaimRecord[]
  readonly coordinatorState: { readonly closed: boolean }
}

/** Result of {@link AgentGraphController.schedule}: the committed update and the post-commit projection. */
export interface AgentGraphControllerScheduleResult {
  readonly update: { readonly revision: number; readonly committedAt: number; readonly created: boolean }
  readonly projection: AgentGraphScheduleProjection
}

/** Result of {@link AgentGraphController.yield}: a pending wake row is durable. */
export interface AgentGraphControllerYieldResult {
  readonly deliveredOnIdle: true
  readonly wakeId: string
}

/**
 * Host-side graph controller: coordinator map + composed snapshot + wake
 * writes. Not a Cordis service by itself — the composition provides one
 * instance under {@link AGENT_GRAPH_CONTROLLER_SERVICE} for the tools.
 */
export class AgentGraphController {
  readonly store: GraphControlStore
  readonly rootSessionId: string

  private readonly newId: () => string
  private readonly options: AgentGraphControllerCoordinatorOptions | undefined
  private readonly observeGraph:
    | ((topology: AgentGraphReconciliationTopology) => Promise<AgentGraphSupervisorObservation>)
    | undefined
  private readonly coordinators = new Map<string, AgentGraphCoordinator>()

  constructor(options: AgentGraphControllerOptions) {
    this.store = options.store
    this.rootSessionId = options.rootSessionId
    this.newId = options.newId
    this.options = options.options
    this.observeGraph = options.observeGraph
  }

  /**
   * Get or create the process-local coordinator for one graph. `overrides`
   * replace the controller-level runtime selections for that graph only.
   * @throws when the runtime selections (executor/recordSource/maxNewActivations) are absent.
   */
  getOrCreate(
    graphId: string,
    overrides?: Partial<AgentGraphControllerCoordinatorOptions>,
  ): AgentGraphCoordinator {
    const existing = this.coordinators.get(graphId)
    if (existing !== undefined) return existing
    const executor = overrides?.executor ?? this.options?.executor
    const recordSource = overrides?.recordSource ?? this.options?.recordSource
    const maxNewActivations = overrides?.maxNewActivations ?? this.options?.maxNewActivations
    if (executor === undefined || recordSource === undefined || maxNewActivations === undefined) {
      throw new AgentGraphToolError(
        'configuration',
        `agent graph ${graphId}: controller needs executor, recordSource, and maxNewActivations to drive a coordinator`,
      )
    }
    const newId = overrides?.newId ?? this.options?.newId ?? this.newId
    const resolveSelectedResultInputs =
      overrides?.resolveSelectedResultInputs ?? this.options?.resolveSelectedResultInputs
    const hydrateInputHandoffs = overrides?.hydrateInputHandoffs ?? this.options?.hydrateInputHandoffs
    const renderPrompt = overrides?.renderPrompt ?? this.options?.renderPrompt
    const coordinator = new AgentGraphCoordinator({
      graphId,
      store: this.store,
      executor,
      recordSource,
      newId,
      maxNewActivations,
      ...(this.observeGraph !== undefined ? { observeGraph: this.observeGraph } : {}),
      ...(resolveSelectedResultInputs !== undefined ? { resolveSelectedResultInputs } : {}),
      ...(hydrateInputHandoffs !== undefined ? { hydrateInputHandoffs } : {}),
      ...(renderPrompt !== undefined ? { renderPrompt } : {}),
    })
    this.coordinators.set(graphId, coordinator)
    return coordinator
  }

  /**
   * Commit one schedule update through the coordinator (store commit then a
   * drive request) and return the post-commit projection.
   */
  async schedule(
    graphId: string,
    request: AgentGraphScheduleUpdateRequest,
  ): Promise<AgentGraphControllerScheduleResult> {
    const result = await this.getOrCreate(graphId).scheduleUpdate(request)
    const projection = await this.readProjection(graphId)
    return {
      update: {
        revision: result.update.revision,
        committedAt: result.update.committedAt,
        created: result.created,
      },
      projection,
    }
  }

  /**
   * Compose the whole-graph derivation snapshot: schedule projection, folded
   * records, readiness, claims, and the coordinator's own closure state.
   */
  async snapshot(graphId: string): Promise<AgentGraphControllerSnapshot> {
    const provisions = await this.store.listOperatorProvisions(graphId)
    const topology = composeProvisionedTopology(graphId, provisions)
    const recordSource = this.options?.recordSource
    const folded = recordSource === undefined
      ? { records: [] as AgentGraphRecord[], omittedPartialCount: 0 }
      : await readCommittedAgentGraphProjection(graphId, topology.operators, recordSource)
    const [updates, claims] = await Promise.all([
      this.store.listScheduleUpdates(graphId),
      this.store.listAgentGraphIntentClaims(graphId),
    ])
    const projection = projectAgentGraphSchedule(graphId, updates)
    const readiness = buildAgentGraphReadinessSnapshot({
      graphId,
      operators: topology.operators,
      edges: topology.edges,
      records: folded.records,
      policies: [],
    })
    return {
      projection,
      records: folded.records,
      omittedPartialCount: folded.omittedPartialCount,
      readiness,
      claims,
      coordinatorState: { closed: this.coordinators.get(graphId)?.isClosed() ?? false },
    }
  }

  /**
   * Yield for a later supervisor checkpoint: durable wake row when work is
   * pending. Pending means any requested work, a live (claimed/executing)
   * intent claim, or non-empty readiness intents — otherwise nothing to yield.
   */
  async yield(graphId: string): Promise<AgentGraphControllerYieldResult> {
    const snapshot = await this.snapshot(graphId)
    if (snapshot.projection.closed) {
      throw new AgentGraphClosedError(graphId, 'yield')
    }
    const pendingWork = snapshot.projection.work.filter(work => work.status === 'requested')
    const liveClaims = snapshot.claims.filter(
      claim => claim.admissionStatus === 'claimed' || claim.admissionStatus === 'executing',
    )
    if (pendingWork.length === 0 && liveClaims.length === 0 && snapshot.readiness.intents.length === 0) {
      throw new AgentGraphNothingToYieldError(
        graphId,
        'no requested work, no live intent claims, and no readiness intents',
      )
    }
    const snapshotVersion = stableHash({
      schemaVersion: AGENT_GRAPH_CONTROLLER_SNAPSHOT_SCHEMA_VERSION,
      graphId,
      revision: snapshot.projection.revision,
      work: snapshot.projection.work.map(work => [work.workId, work.status]),
      claims: snapshot.claims.map(claim => [claim.intentId, claim.admissionStatus]),
      intents: snapshot.readiness.intents.map(intent => intent.intentId),
    })
    const wakeId = graphWakeId(graphId, snapshotVersion)
    const { wake } = await this.store.claimSupervisorWake({
      graphId,
      wakeId,
      snapshotVersion,
      rootSessionId: this.rootSessionId,
    })
    return { deliveredOnIdle: true, wakeId: wake.wakeId }
  }

  /**
   * Stop one work target: validated against the live projection, committed as
   * a stop-only schedule update, then re-driven.
   */
  async stop(
    graphId: string,
    target: AgentGraphScheduleStop,
    source: AgentGraphScheduleUpdateSource,
  ): Promise<AgentGraphControllerScheduleResult> {
    const snapshot = await this.snapshot(graphId)
    const known = snapshot.projection.work.some(work => work.workId === target.targetId)
    if (!known) {
      throw new AgentGraphInvalidInputError(
        `agent graph ${graphId}: stop target ${target.targetId} is not an existing work in the live projection`,
      )
    }
    const updateId = graphUpdateId(graphId, source)
    const commitSource: AgentGraphScheduleUpdateSource = { ...source, orchestrationMode: 'graph' }
    const request: AgentGraphScheduleUpdateRequest = {
      schemaVersion: AGENT_GRAPH_TOOL_UPDATE_SCHEMA_VERSION,
      updateId,
      updateFingerprint: stableHash({
        schemaVersion: AGENT_GRAPH_TOOL_UPDATE_SCHEMA_VERSION,
        updateId,
        graphId,
        source: commitSource,
        addWork: [],
        stop: [target],
      }),
      graphId,
      source: commitSource,
      addWork: [],
      stop: [target],
    }
    return this.schedule(graphId, request)
  }

  private async readProjection(graphId: string): Promise<AgentGraphScheduleProjection> {
    const updates = await this.store.listScheduleUpdates(graphId)
    return projectAgentGraphSchedule(graphId, updates)
  }
}

/** Construct a host-side graph controller. */
export function createAgentGraphController(
  options: AgentGraphControllerOptions,
): AgentGraphController {
  return new AgentGraphController(options)
}
