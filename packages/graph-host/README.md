---
description: "Agent Graph host assembly: wires the P1-P5 slices to real harness services (storage, subagents, worktrees, compaction, idle) and provides the agentGraphController service the supervisor tools consume."
kind: "package-reference"
---

# @hy-sde-org/dsh-graph-host

English | [中文](README.zh.md)

## Summary

`dsh-graph-host` is the host-plane assembly of the Agent Graph (Maka port, slices P6–P7a): it opens the graph control unit (P1 `dsh-graph-control`), builds the operator executor (P3 `dsh-graph-executor`) over real harness seams, feeds the identity-less P3 record sink through a run-identity ledger, constructs the `AgentGraphController` (P4 `dsh-tool-graph`) the supervisor tools run on, and drives wake delivery (P5 `dsh-graph-wakes`) on the graph root session's idle boundaries. The package contributes no tool and no prompt section — the model-facing surface stays in `dsh-tool-graph`; this package supplies the services and the durable `graph/change` event stream.

The `graph-host` Cordis plugin declares `inject: ['agents', 'sessions', 'subagents', 'git', 'compaction']` and publishes two services on its own fiber:

| service | exported constant | value |
| --- | --- | --- |
| controller the supervisor tools resolve with `ctx.get` | `SERVICE_AGENT_GRAPH_CONTROLLER` | `AGENT_GRAPH_CONTROLLER_SERVICE` from `dsh-tool-graph` = `'agentGraphController'` |
| the whole assembly handle | `SERVICE_GRAPH_HOST` | `'graphHostServices'` |

The plugin `Config` fields: `rootSessionId` (required — the graph root; only this session may drive supervisor tools and it owns the `graph/change` events), `subagentProvider` (required — provider name forwarded to `subagents.start`; the shipped in-process provider is `spawn`), `backend?` (storage backend name hosting the graph control unit, default `'sqlite'`), `worktreeRepoRoot?` / `worktreeBaseBranch?` / `worktreeMaxSlots?` (worktree pool geometry), and `maxNewActivations?` (cap on new operator activations per reconcile drive, default `4`).

## Table of Contents

- [Use this package](#use-this-package)
- [Understand the implementation](#understand-the-implementation)
- [Further Exploration](#further-exploration)
- [Model Experience](#model-experience)
- [Known Limitations and Deferred Work](#known-limitations-and-deferred-work)

## Use this package

The host row lives in the **host composition** — it injects host services and publishes new ones, so nothing about it can be keyed by session. The tool row lives in the **agent preset** of the graph root session; it only consumes the published controller.

```yaml
# host composition (loaded before any session)
- id: graph-host
  name: '@hy-sde-org/dsh-graph-host'
  config:
    rootSessionId: session-01HABC   # the graph root session id
    subagentProvider: spawn         # one-shot in-process subagent provider
    backend: sqlite                 # backend owning the graph control unit
    maxNewActivations: 4            # new operator activations per drive (default 4)
```

```yaml
# agent preset composition for the graph root session
- id: tool-graph
  name: '@hy-sde-org/dsh-tool-graph'
```

The plugin mounts the assembly when the root agent publishes (`agent/created`) or immediately when it is already live, and withdraws the services with its fiber. The assembly can also be built explicitly:

```ts
import { createGraphHostServices } from '@hy-sde-org/dsh-graph-host'

const services = await createGraphHostServices({
  rootSessionId: session.id,
  storage: { open: descriptor => backend.kv.open(descriptor) },
  subagents: { provider: 'spawn', start: (name, request) => ctx.subagents.start(name, request) },
  worktrees: {
    repoRoot,
    acquire: options => worktreeEngine.acquire(options),
    list: () => worktreeEngine.list(),
  },
  compaction: { request: sessionId => agent.runMaintenance(signal => engine.compactNow(agent, signal).then(() => {})) },
  sessionEvents: { appendGraphChange: (sessionId, data) => session.append('graph/change', data).then(() => true) },
  idle: { observe: (rootSessionId, onIdle) => agent.ctx.on('agent/status', ({ status }) => {
    if (status === 'idle') onIdle(rootSessionId)
  }) },
  resolveParentAgent: rootSessionId => ctx.agents.get(SessionId(rootSessionId)),
})

await services.attachGraph('graph_g1') // registers the controller, starts the wake runtime
const snapshot = await services.snapshotFor('graph_g1') // bounded P6 session projection
await services.emitGraphChange(session.id, 'graph_g1', snapshot, snapshot.revision)
await services.dispose() // stops wake delivery, cancels children, closes the store
```

### Wiring

The repository ships the opt-in composition patch [`apps/cli/config/examples/graph/cordis.yml`](../../../apps/cli/config/examples/graph/cordis.yml). Apply it from a development checkout with:

```sh
dsh web --patch apps/cli/config/examples/graph/cordis.yml
```

The patch carries the `graph-host` row above with `rootSessionId` left as the `<ROOT_SESSION_ID>` placeholder and `subagentProvider: spawn`; replace the placeholder with the deployment's graph root session id. The preset rows (`@hy-sde-org/dsh-tool-graph`, `@hy-sde-org/dsh-graph-projection`) mount in the graph root session's agent preset, as shown above for `tool-graph`.

## Understand the implementation

### Plane split: host row against preset row

`graph-host` is a **host-plane row**: it injects `agents`, `sessions`, `subagents`, `git`, and `compaction` and publishes `agentGraphController` plus `graphHostServices`. Injection resolves before any session exists, so there is no agent to key by — publishing from a preset realm would hide the service from the host and from every other session. `dsh-tool-graph` is the **agent-plane row**: it consumes the controller opportunistically with `ctx.get` and fails loud at load when it is absent. A preset row must never publish a service; the only preset-row contribution here is tool registration.

### Facades over real services

Every service the assembly needs is narrowed to a structural facade in `src/types.ts`, so the assembler and its tests never widen to full service classes. The plugin maps each facade: storage backend (`ctx.get(storageBackendServiceKey(name))`), subagents (`ctx.subagents.start`), git worktree engine (`primaryRepoRoot` + `acquireWorktree`/`listWorktrees`), compaction (`ctx.compaction` through the agent's maintenance seam), session events (`session.append('graph/change', …)`), and idle (`agent/status === 'idle'` observations forwarded to the wake runtime).

### Exactly-once activation guard

The P3 executor serializes activations per operator but does not memoize claims, so the assembler wraps it in `OncePerClaimGraphExecutor`: one claim id gets one child run per assembly, and a re-drive after a finished (or in-flight) activation returns the folded terminal records without starting a second child. The guard is process-local — the durable claim row remains the restart authority.

### Record fold and the no-durable-record decision

The P3 `recordSink` receives `AgentGraphRecordSourceEvent` **without** operator/session identity (the event carries `runId` only), so `GraphRunIdentityLedger` restores the identity from the child start the executor itself performed (the executor copies `claim.targetRunId` into both the start input and the emitted event), and `InProcessGraphRecordSource` folds the terminal event keyed by operator×session. `readCommittedAgentGraphProjection` (P2) replays that source exactly as it replays child-session logs, so record derivation stays P2-identical within one host process.

Records are derived state, and this slice keeps them **in process memory on purpose**: a durable `graph/record` event on a child session would be refused by the persistence read path until `KNOWN_SESSION_EVENT_TYPES` is regenerated (P6 scope), and attributing a terminal event to a child-session log needs a P3 change. The durable authority is the set of control rows — schedule, claims, provisions, wakes. **A host restart loses operator records** until a later slice adds the attributed durable event; the session projection then shows the schedule/claim state (e.g. `claimed`) instead of the terminal outcome until that slice lands.

### Session projection and the graph/change contract

`buildSessionGraphProjection` turns the controller's whole-graph snapshot into the bounded P6 payload. Every `graph/change` event carries `{ graphId, snapshot, revision }`:

```ts
interface SessionGraphProjection {
  readonly schemaVersion: 1
  readonly graphId: string
  readonly status: 'active' | 'closed'
  readonly revision: number
  readonly closed: boolean
  readonly work: readonly {
    readonly workId: string
    readonly status: 'requested' | 'claimed' | 'executing' | 'stopped' | 'finished' | 'failed'
    readonly instruction: string // truncated to 300 characters with a trailing ellipsis
    readonly operatorId?: string
    readonly inputCount: number
  }[]
  readonly omitted: { readonly work: number; readonly records: number; readonly inputs: number }
  readonly pendingWake: boolean
  readonly updatedAt: number
}
```

Work entries are capped (`SESSION_PROJECTION_MAX_WORK = 128`: requested head plus terminal tail), records tailed to `SESSION_PROJECTION_MAX_RECORDS = 64`, instructions to `SESSION_PROJECTION_INSTRUCTION_MAX_CHARS = 300`; `omitted` carries the excluded counts. `pendingWake` is true while the graph has a `pending` or `retryable_failed` wake row. Status precedence: a schedule stop wins first; then the operator's terminal record (the executor folds it only after the child settled — `[operator failed] …`/`[operator cancelled]` summaries map to `failed`/`stopped`, anything else to `finished`); then the claim admission state (in-flight only); then `requested`. Terminal records outrank claim admission because the admission status has no terminal state.

`emitChange` (called after every schedule commit, every reconciliation, and every wake delivery) is best-effort, deduped by a content fingerprint, and serialized per graph so concurrent triggers emit at most one event per fingerprint change. The event is appended to the root session's log only — no surface placement — and the P6 projection layer folds it.

### Wake delivery

`attachGraph` registers the controller and starts the wake runtime scoped to the root session. Delivery runs only at idle boundaries (`observeIdle` → `handleIdle`): the deliver hook re-drives the coordinator and emits a fresh `graph/change`. A closed graph short-circuits to `superseded` (the runtime also folds the schedule log itself: a `finish` update or a graph/root-targeted stop supersedes the wake without delivery). A failed delivery returns `retryable_failed`; a provider-confirmed context overflow (`GraphHostContextOverflowError` or an `overflow === true` marker) triggers the runtime's one-compaction recovery, and a second overflow of the same wake carries the bounded partial snapshot (`partialResult: true`) so the wake exhausts at the attempt cap (`DEFAULT_MAX_DELIVERY_ATTEMPTS = 3`) instead of retrying a third full delivery. The post-reconcile projection read is part of the delivery: an overflow-marked failure there triggers the same one-compaction path.

## Further Exploration

- [dsh-graph-control](../graph-control/README.md) (P1) — the durable store (schedule log, claims, provisions, wakes) this assembly opens.
- [dsh-graph-stream](../graph-stream/README.md) (P2) — the coordinator and the record-projection fold the executor and controller drive.
- [dsh-graph-executor](../graph-executor/README.md) (P3) — the operator executor and its child-runner/worktree-pool seams.
- [dsh-tool-graph](../tool-graph/README.md) (P4) — the supervisor tools over `agentGraphController`.
- [dsh-graph-wakes](../graph-wakes/README.md) (P5) — the idle-gated wake runtime this assembly wires.
- [dsh-graph-projection](../graph-projection/README.md) (P6) — folds `graph/change` events into the session graph projection.
- `tests/graph-host.spec.ts` — real SQLite store with fake subagents/worktrees/compaction/session-events/idle.

## Model Experience

No model-facing prompt text originates here. The model sees the three supervisor tools of `dsh-tool-graph`; this package's contributions are the controller service they run on and the `graph/change` events appended to the root session log (durable input to the P6 projection, not a surface element). No KV-cache or token effect: the executor does not call a model provider itself, it starts subagent children through the injected seam.

## Known Limitations and Deferred Work

- **Operator records are process-local.** The P3 record sink carries no operator/session identity and the durable child-session event would be refused until `KNOWN_SESSION_EVENT_TYPES` is regenerated, so `InProcessGraphRecordSource` + `GraphRunIdentityLedger` fold terminal records in memory only: a host restart loses them, and the session projection falls back to schedule/claim state until a later slice adds the attributed durable event.
- **The exactly-once activation guard is process-local.** `OncePerClaimGraphExecutor` memoizes by claim id in memory; after a restart a re-driven claim can start a second child run because the durable claim row records admission but not the terminal outcome.
- **`graph/change` dedup is per-assembly and in-memory**: the fingerprint map resets on restart, so a restarted host may re-emit one event for an unchanged graph.
- **One root session per mount**: the plugin Config names a single `rootSessionId` (the wake runtime and the emission target are scoped to it), and the `agentGraphController` service name is a singleton per host fiber — a deployment with several graph roots needs per-root host rows in separate realms rather than one shared row.
- **Terminal work items stay `requested` in the durable schedule log** until a stop/finish update commits; the session projection derives `finished` from the folded terminal record, so a host restart that loses records shows `claimed`/`executing` again (see the record-fold limitation).
