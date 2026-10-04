<!-- MIRROR-NOTE:START -->
> [!NOTE]
> 📦 This plugin lives in the [**dsh-plugins**](https://github.com/hy-sde/dsh-plugins) monorepo — file issues & pull requests there.
> npm: [`@hy-sde-org/dsh-graph-control`](https://www.npmjs.com/package/@hy-sde-org/dsh-graph-control) · [`@hy-sde-org/dsh-graph-executor`](https://www.npmjs.com/package/@hy-sde-org/dsh-graph-executor) · [`@hy-sde-org/dsh-graph-host`](https://www.npmjs.com/package/@hy-sde-org/dsh-graph-host) · [`@hy-sde-org/dsh-graph-projection`](https://www.npmjs.com/package/@hy-sde-org/dsh-graph-projection) · [`@hy-sde-org/dsh-graph-stream`](https://www.npmjs.com/package/@hy-sde-org/dsh-graph-stream) · [`@hy-sde-org/dsh-graph-wakes`](https://www.npmjs.com/package/@hy-sde-org/dsh-graph-wakes) · [`@hy-sde-org/dsh-tool-graph`](https://www.npmjs.com/package/@hy-sde-org/dsh-tool-graph)
<!-- MIRROR-NOTE:END -->

# dsh-graph — durable agent-graph scheduling for DeepSeek Harness

A standalone port of the Maka Agent Graph family (control store, stream core, executor, supervisor tools, wakes, projection, host) for the [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) — durable dependency scheduling with exactly-once claims and supervisor yield/wake. The graph root session plans a durable work schedule (`view_agent_graph` / `update_agent_graph`) and ends its turn with `yield_agent_graph` instead of polling; operator children run as subagents inside git-worktree leases under exactly-once claims; and the host wakes the supervisor at the next idle boundary from the durable wake rows.

| Identity | Value |
| --- | --- |
| Packages | 7 published npm packages (see [Packages](#packages)); the repo root is a private workspace (`dsh-graph` `0.1.7-rc.2`) with no umbrella npm package |
| Plugin row ids | `graph-host` (host composition) · `tool-graph` (graph root session's agent preset) · `graph-projection` (beside the session projection registry) |
| Tools | `view_agent_graph` · `update_agent_graph` · `yield_agent_graph` |
| Prompt section | `orchestration:graph` |
| Service ids | `agentGraphController` · `graphHostServices` |
| Projection unit | `graph` (fed by `graph/change` session events) |
| Storage unit | `agent_graph` `KvUnit` (`@deepseek-ai/dsh-storage`) |

> **Based on [Apache Maka](https://github.com/apache/maka) (Apache-2.0)** — the Agent Graph control store, stream core, executor adapter, supervisor tools, wake delivery, projection, and host plugin are ported from Maka (No. 1) and re-hosted on DeepSeek Harness primitives; the modules also derive from the [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) codebase (MIT). Provenance is aggregated in [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).

## Packages

| Package | Role |
| --- | --- |
| [`@hy-sde-org/dsh-graph-control`](packages/graph-control/README.md) | Durable decision store: append-only schedule log, exactly-once intent claims, operator provisions, supervisor wakes |
| [`@hy-sde-org/dsh-graph-stream`](packages/graph-stream/README.md) | Derivation layer: records, trace, readiness, input handoffs, and the `AgentGraphCoordinator` drive loop |
| [`@hy-sde-org/dsh-graph-executor`](packages/graph-executor/README.md) | Operator executor adapter: deterministic worktree leases, durable bindings, serialized child runs with terminal records |
| [`@hy-sde-org/dsh-tool-graph`](packages/tool-graph/README.md) | Model-facing supervisor tools and the `orchestration:graph` prompt section |
| [`@hy-sde-org/dsh-graph-wakes`](packages/graph-wakes/README.md) | Idle-gated supervisor wake delivery over the durable wake rows |
| [`@hy-sde-org/dsh-graph-projection`](packages/graph-projection/README.md) | `graph` session-projection unit folding the host's whole-value `graph/change` publishes |
| [`@hy-sde-org/dsh-graph-host`](packages/graph-host/README.md) | Host-plane assembly: opens the store, builds the executor over real harness seams, publishes the controller |

## Why

An agent coordinating long fan-out work through conversation alone loses the plan on a restart, a compaction, or a busy turn. This group makes the schedule durable instead: every decision is an append-only schedule row with a deterministic sha256 id (`graph_update_…`, `graph_claim_…`, `graph_operator_…`, `graph_wake_…`), and the exactly-once claim — written with preallocated turn/run identity at the schedule revision it observed — means a retry reuses the same activation identity instead of invoking the provider twice. Every claim/provision transition is conditional on the observed schedule revision, so a stale drive cannot re-dispatch. The store is the authority; records, routes, readiness intents, and work status stay derived and recomputable. The supervisor reports outcomes per wave and yields, and the host re-drives the graph and wakes the root session at a durable checkpoint — no polling.

## Prerequisites

- Node.js 22.19 or newer with npm and pnpm on `PATH` (repo `engines`: `>=22.19.0`);
- a DeepSeek Harness installation providing the group's peer ranges — `@deepseek-ai/*` seam packages at `^0.2.0-rc.2` (storage, session, session-projection, session-persistence, agent, subagent, compaction, llm, tools, system-prompt) and `@deepseek-ai/cordis` `~4.0.4` — including the standard `dsh` CLI;
- the published `@hy-sde-org/dsh-git` worktree engine (a `graph-host` peer);
- a storage backend for the graph control unit (default `sqlite`) — one `agent_graph` `KvUnit`, opened exactly once per process.

Install the CLI and pnpm before continuing:

```bash
npm install --global @deepseek-ai/dsh pnpm
dsh --version
```

## Quick start

### Route A — published npm packages (recommended)

```bash
dsh plugin --profile web add @hy-sde-org/dsh-graph-host
dsh plugin --profile web add @hy-sde-org/dsh-tool-graph
dsh plugin --profile web add @hy-sde-org/dsh-graph-projection
```

The four library packages (`dsh-graph-control`, `dsh-graph-stream`, `dsh-graph-executor`, `dsh-graph-wakes`) ship no plugin row; they are peer dependencies of the host row. All peer dependencies are published packages — nothing here requires unpublished fork packages.

### Route B — from source (validate this checkout or hack on the plugins)

```bash
git clone git@github.com:hy-sde/dsh-plugins.git
cd dsh-plugins/dsh-graph
pnpm install
for pkg in graph-host tool-graph graph-projection; do
  (cd packages/$pkg && PACKAGE_TARBALL="$(pnpm pack | tail -n 1)" && dsh plugin --profile web add "$PWD/$PACKAGE_TARBALL")
done
```

Each `pnpm pack` runs the package's normal `prepack` build and produces a tarball containing `dist/`. A direct `github:<this-repo>` dependency does not contain built output and is not a supported install path — always install the built tarballs (or the published packages).

### Wire the rows

The group ships no bundle patch: mount by adding the package rows to a profile composition (see the harness plugin docs). The `graph-host` row lives in the **host composition** — it injects host services and publishes new ones, so nothing about it can be keyed by session. The `tool-graph` and `graph-projection` rows live in the **agent preset** of the graph root session:

```yaml
# host composition (loaded before any session)
- id: graph-host
  name: '@hy-sde-org/dsh-graph-host'
  config:
    rootSessionId: <ROOT_SESSION_ID>   # the graph root session id
    subagentProvider: spawn            # one-shot in-process subagent provider
    backend: sqlite                    # backend owning the graph control unit
    maxNewActivations: 4               # new operator activations per drive (default 4)
```

```yaml
# agent preset composition for the graph root session
- id: tool-graph
  name: '@hy-sde-org/dsh-tool-graph'
- name: '@deepseek-ai/dsh-session'
- name: '@deepseek-ai/dsh-session-projection'
- name: '@hy-sde-org/dsh-graph-projection'   # registers only when the registry is present
```

Replace `<ROOT_SESSION_ID>` with the deployment's graph root session id — only that session may drive the supervisor tools, and it owns the `graph/change` events. A preset row must never publish a service; the only preset-row contributions here are tool registration and the projection unit.

### Verify the composed configuration

```bash
dsh web --dump-config
```

The composed tree must show a `graph-host` row loading `@hy-sde-org/dsh-graph-host` in the host composition and a `tool-graph` row loading `@hy-sde-org/dsh-tool-graph` in the graph root session's preset, with `@hy-sde-org/dsh-graph-projection` beside the session projection registry.

### Run

```bash
dsh web
```

In the graph root session, ask the agent to plan work: `update_agent_graph` commits one durable decision per call (add work via `targetKind`, stop targets, or finish with committed result ids; `idempotencyKey` makes retries safe), and `yield_agent_graph` ends the supervisor turn while the graph drives operator children. The host wakes the session at the next durable checkpoint; `view_agent_graph` shows the bounded schedule — work statuses, truncated record summaries, readiness intents, explicit `omitted` counts.

### Uninstall

This group ships no bundle row — the graph-host row (host composition) and the
`dsh-tool-graph` / `dsh-graph-projection` rows (agent preset) were added by
hand, so `dsh plugin remove` does not touch them. Remove those rows first,
then:

```bash
dsh plugin --profile web remove @hy-sde-org/dsh-graph-host
dsh plugin --profile web remove @hy-sde-org/dsh-tool-graph
dsh plugin --profile web remove @hy-sde-org/dsh-graph-projection
```

## What the bundle does

The group ships no bundle patch — the three mountable packages declare Cordis plugin rows you place yourself:

1. **`graph-host` (host-plane row)** injects `agents`, `sessions`, `subagents`, `git`, `compaction`; opens the graph control unit; builds the operator executor over the git worktree engine and the subagent runtime (wrapped in the `OncePerClaimGraphExecutor` exactly-once guard); publishes the `agentGraphController` service the supervisor tools resolve with `ctx.get` plus the whole `graphHostServices` assembly handle; appends `graph/change` events to the root session log after every schedule commit, reconciliation, and wake delivery; and drives wake delivery on the root session's idle boundaries. It mounts when the root agent publishes (`agent/created`) or immediately when already live, and withdraws its services with its fiber.
2. **`tool-graph` (agent-plane row)** registers the three supervisor tools and the `orchestration:graph` prompt section in the graph root session. The tools mount even when the controller is absent — an agent preset never breaks session creation — and each call fails loud with `[agent-graph-unavailable]` until the host provides it.
3. **`graph-projection`** registers the `graph` session-projection unit: a pure fold over the host's whole-value `graph/change` publishes (serving `null` until the first publish). The web chat renders it through `useProjection('graph')` as the graph rail and chip. Without the projection registry the unit registers nothing, and other assemblies simply serve no `graph` key.

The four libraries contribute no tool, prompt, or plugin row — the control store, coordinator, executor adapter, and wake runtime are consumed by the host assembly.

### Faithful port of Maka, adapted to the harness

Per [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md), the control store, stream core, executor adapter, supervisor tools, wake delivery, projection, and host plugin are ported from Apache Maka (No. 1) and re-hosted on DSH primitives, with additional derivation from the DeepSeek Harness codebase.

**Faithful to Maka:**

- The one hard rule: the durable claim — with preallocated turn/run identity — is written before the runtime is ever asked to run, and every claim/provision transition is conditional on the schedule revision it observed, so a retry reuses the same activation identity.
- Deterministic sha256 ids cut to 32 hex chars make replays idempotent by construction; identity ordering is UTF-16 code-unit order, stable across processes.
- The authored drive loop (provision → supervise → select → render → execute), the supervisor wake state machine (`pending → running → delivered/superseded/retryable_failed/exhausted`), and Maka's payload bounds (32 addWork items, 64 input ids, 64 selected results, 60 000 instruction chars, 20 stop targets, 64 finish result ids, 4 000 reason chars).
- Worktrees intentionally survive terminal runs — the executor never releases a lease; pool release is wired only for graph teardown.
- One documented deliberate deviation: the record is a slim copy-with-provenance shape, not Maka's 18-facet full record.

**Adapted to the harness (and why):**

- **Control store.** Maka's storage layer becomes one `agent_graph` `KvUnit` over `@deepseek-ai/dsh-storage` — five authoritative tables plus the `operator_bindings` table the executor adapter added — serialized on one write chain with derived uniqueness indexes rebuilt at open, so a torn write heals instead of corrupting.
- **Worktree leases.** Lease identity is derived (`graph_operator_lease_<sha256(graphId, workId, provisionFingerprint)[32]>`) and made durable through the `operator_bindings` binding row — the durable hint a real pool consults after a restart, since lease idempotency is otherwise process-local.
- **Execution seams.** Children run through the harness subagent runtime (`subagents.start`, shipped provider `spawn`) instead of Maka's session manager; the coordinator never calls a provider directly.
- **Wake delivery.** Maka's supervisor wake is re-expressed as a host runtime gated on the harness `agent/status === 'idle'` observation seam (the same pattern the Schedule package uses), so a wake is never delivered while a turn is running.
- **Tool surface.** DSH has no `direct_only`/`nesting` tool flag, so root-only enforcement lives in the tool body (`call.sessionId` must equal the controller's `rootSessionId`), and Maka's run list is mapped to the agent-loop turn boundary (`graph_run_<n>` / `graph_turn_<n>`) — documented as not Maka-identical.
- **Records stay process-local.** The durable authority is the set of control rows (schedule, claims, provisions, wakes); terminal operator records fold in memory, and a host restart loses them until the host wiring adds the attributed durable event — the session projection then falls back to schedule/claim state.

## Configuration

The only configurable row is `graph-host`; `tool-graph` takes no options. Override fields via the row's `config:` — the wiring example under [Quick start](#quick-start) is the complete wiring.

| Option | Default | Purpose |
| --- | --- | --- |
| `rootSessionId` | required | the graph root session id; only this session may drive the supervisor tools, and it owns the `graph/change` events |
| `subagentProvider` | required | provider name forwarded to `subagents.start`; the shipped in-process provider is `spawn` |
| `backend` | `sqlite` | storage backend name hosting the graph control unit |
| `worktreeRepoRoot` / `worktreeBaseBranch` / `worktreeMaxSlots` | — | worktree pool geometry |
| `maxNewActivations` | `4` | cap on new operator activations per reconcile drive |

Wake retry behavior is fixed in code, not config: a `retryable_failed` wake re-arms at `now + 30 s × attemptNumber` and durably exhausts at `DEFAULT_MAX_DELIVERY_ATTEMPTS = 3`.

## Compatibility

| Component | Supported contract |
| --- | --- |
| Node.js | 22.19 or newer |
| DeepSeek Harness | `^0.2.0-rc.2` peer range (`dsh-storage`, `dsh-session`, `dsh-session-projection`, `dsh-session-persistence`, `dsh-agent`, `dsh-subagent`, `dsh-compaction`, `dsh-llm`, `dsh-tools`, `dsh-system-prompt`); `@deepseek-ai/cordis` `~4.0.4` |
| Sibling package | `@hy-sde-org/dsh-git` — the worktree engine (`graph-host` peer) |
| Storage | one `agent_graph` `KvUnit`; single-writer contract — open the unit exactly once per process |
| Seams | `subagents`, git worktree engine (`acquire`/`list`), `compaction` maintenance, session events (`graph/change`), `agent/status === 'idle'` observations; the projection registry is optional (without it, no `graph` key is served) |

Design scope: one DSH session owns one graph — multi-graph-per-root is deferred — and the `agentGraphController` service name is a singleton per host fiber, so deployments with several graph roots need per-root host rows in separate realms. DeepSeek Harness is a developer preview; upstream seam-contract changes require a new package release and contract review.

## Development

```bash
pnpm install
pnpm check      # strict typecheck across all packages
pnpm test       # vitest suites per package
pnpm build      # tsc -> dist for every package
pnpm pack:all   # pack every package (runs each prepack build)
```

Each package also has its own `clean`/`build`/`check`/`test`/`prepack`. The suites run against real SQLite-backed stores: graph-stream exercises projection, validation, handoff, and end-to-end drive scenarios; graph-executor pairs a fake pool/runner with a real store; graph-wakes uses a fake idle observer; graph-host fakes the subagents/worktrees/compaction seams; graph-control tests the store contracts directly. Each sub-package README (linked under [Packages](#packages)) documents its surface in depth.

## License and attribution

This repository is licensed MIT (© 2026 hy-sde, see [LICENSE](LICENSE)). The modules are derived from the [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) codebase (MIT, © 2026 DeepSeek) and from [Apache Maka](https://github.com/apache/maka) (Apache-2.0, © the Apache Software Foundation — "Apache Maka Incubating"); per [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md), the Apache License 2.0 text applies to the derived Maka modules while the rest of the repository is MIT. Maka references cited across the sub-READMEs: the agent-graph stream scheduling draft (Chapter 7), `git-worktree-child-executor.ts`, `session-manager.ts`, `agent-graph-supervisor-wake.ts`, and `stream-graph-supervisor-tools.ts`.

These packages are separate installable components; the harness remains the property of its own project.
