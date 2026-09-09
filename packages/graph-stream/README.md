---
description: "Derived stream layer for the Agent Graph: deterministic identities, record/trace/readiness/schedule projections, handoffs, and the process-local reconciliation coordinator."
kind: "package-reference"
---

# @hy-sde-org/dsh-graph-stream

English | [中文](README.zh.md)

## Summary

`dsh-graph-stream` is the derivation layer of the Agent Graph (Maka port, slice P2). It layers on top of `@hy-sde-org/dsh-graph-control` (the durable decision store, P1) and owns **everything that can be recomputed from committed rows**: work-status projection, record folding, trace/route derivation, readiness intents, input handoffs, and the single-flight reconciliation driver (`AgentGraphCoordinator`) that walks Maka's authored drive loop (provision → supervise → select → render → execute) against that store.

The split follows Maka's one hard rule: the store is the authority, the stream layer never writes anything except through the store's own commit/claim/provision methods. Every projection here is deterministic and pure — recomputing it never starts work; admission and execution are store-sealed (claim with preallocated turn/run identity at the schedule revision it observed).

Ids are deterministic sha256 cut to 32 hex chars (`graph_intent_…`, `graph_operator_…`, `graph_edge_…`, `graph_route_…`, `graph_record_…`, `graph_claim_…`), so replays are idempotent by construction. Identity comparison uses UTF-16 code-unit order (`compareAgentGraphIdentity`), not locale comparison.

This package contributes no tool, prompt, or plugin row — the executor adapter (P3) and supervisor tools (P4) consume it.

## Table of Contents

- [Use this package](#use-this-package)
- [Understand the implementation](#understand-the-implementation)
- [Further Exploration](#further-exploration)
- [Model Experience](#model-experience)
- [Known Limitations and Deferred Work](#known-limitations-and-deferred-work)

## Use this package

```ts
import Storage, { storageBackendServiceKey } from '@deepseek-ai/dsh-storage'
import { GraphControlStore } from '@hy-sde-org/dsh-graph-control'
import { AgentGraphCoordinator } from '@hy-sde-org/dsh-graph-stream'

const backend = await ctx[storageBackendServiceKey('sqlite')]
const unit = await backend.kv.open(GraphControlStore.descriptor)
const store = await GraphControlStore.open(unit)

const coordinator = new AgentGraphCoordinator(graphId, {
  store,
  executor: { provisionOperator, runClaimedAgentGraphIntent, stopSession },
  recordSource,
  newId,
})

await coordinator.scheduleUpdate({ graphId, addWork: [work] }) // commits, then wakes the drive
const result = await coordinator.reconcileAndWait()             // runs one drive to idle
```

## Understand the implementation

### Deterministic identities and ordering

`stableHash(value)` = `sha256:` + hex of canonical JSON (sorted object keys, `undefined`/function/symbol → `"[undefined]"`, bigint → numeric string, `required`/`enum` arrays sorted with `localeCompare`, Date → ISO). All ids use the first 32 hex chars. `compareAgentGraphIdentity` is UTF-16 code-unit order — stable across processes, unlike `localeCompare`.

### Projections

- **Schedule projection** (`projectAgentGraphSchedule`): folds the append-only update log into the model-visible work view; validates contiguity (revisions from 1), no updates after a finish, no repeated work ids, wrong graph ids rejected. `stopped` wins over `superseded`.
- **Record fold** (`readCommittedAgentGraphProjection`): derives reference-only records from committed events per (operator, session); partial events skipped; at most one terminal record per activation; deterministic order.
- **Trace** (`validateAgentGraphTraceTopology`, `buildAgentGraphTraceSnapshot`): validates the DAG (duplicate ids/endpoints, self-loops, unknown operators, cycles via Kahn) and derives one route per (record × outgoing edge).
- **Readiness** (`buildAgentGraphReadinessSnapshot`): map policy — one intent per route received through a declared incoming edge, sealed against exactly the triggering records (`policyFingerprint`, `readinessContextFingerprint`, `graph_intent_…` via stable hashes).
- **Handoff** (`hydrateAgentGraphInputHandoffs`, `renderAgentGraphScheduledWorkPrompt`): resolves bounded conclusion text (16 KiB/record, 48 KiB total, `…` ellipsis, binary search over code points) and renders the operator prompt: instruction + `GRAPH_OPERATOR_HANDOFF_PROTOCOL` + `<agent_graph_input_handoffs>` with `<` escaped as `\u003c`. Records stay reference-only; text is resolved only at render time.

### Reconciliation and the coordinator

`reconcileAgentGraphSchedule` walks Maka's phases: A provision operators → B derive supervisor intents → C select (existing claims always; new capped by `maxNewActivations`, excess → `activation_limit`) → D render (all-or-fail) → E execute (claim at revision → run with `admitExecution` = begin-execution at revision). Statuses: `reconciled | waiting | limit_reached | failed | cancelled | stale`. `applyScheduleStops` replaces (`status: 'superseded'`), cancels claims, and stops sessions in batches.

`AgentGraphCoordinator` is the process-local single-flight driver: `scheduleUpdate` commits a row then wakes the drive; `reconcileAndWait` joins exactly one drive; `recover` resumes graphs with a non-empty schedule after restart; `stop`/`wake`/`isClosed` surface the same lifecycle. A drive re-runs while work or stopped targets remain, and observes existing claims as already-dispatched (executor dedupes by claim id).

### Executor seam

```ts
export interface AgentGraphExecutor {
  provisionOperator(request: AgentGraphOperatorProvisionRequest): Promise<AgentGraphOperatorProvisionResult | undefined>
  runClaimedAgentGraphIntent(input: AgentGraphRunClaimedIntentInput): Promise<void>
  stopSession(sessionId: string, opts?: { reason?: string }): Promise<void>
}
```

The coordinator never calls a provider directly — P3 supplies the subagent/worktree-backed implementation.

## Further Exploration

- `packages/graph/graph-control` (P1): the durable rows this layer folds.
- `src/reconcile.ts` / `src/coordinator.ts`: the drive loop and status derivation.
- `src/hash.ts` / `src/identity.ts`: the canonicalization and ordering primitives every id and fingerprint depends on.
- `tests/graph-stream.spec.ts` / `tests/reconcile.spec.ts`: projection, validation, handoff, and end-to-end drive scenarios against a real sqlite-backed store.

## Model Experience

The package is pure TypeScript with narrow, single-responsibility modules and no ambient state. Types are explicit; results are plain data (no class instances crossing module boundaries except the coordinator). Errors carry `reason` codes for validation failures and the store's conflict errors propagate unchanged.

## Known Limitations and Deferred Work

- Readiness policy kinds: `map` only — `all_settled` and supervisor-readiness kinds are deferred to P4.
- No client projection/checkpointing (`onCheckpoint`) yet; no tool-view pagination; residency is a no-op.
- Map-policy intents are derived but not auto-dispatched by reconcile — they surface for supervisor tools in P4.
- Record shape is copy-with-provenance (slim), a deliberate deviation from Maka's 18-facet full record; the stream layer never mutates stored records.
- The coordinator is process-local: another process holding the same graph store will not wake this driver automatically (wake delivery is P5).
