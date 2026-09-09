---
description: "Agent Graph supervisor surface: three root-only tools (view/update/yield), the host-side graph controller, and the orchestration:graph prompt section."
kind: "package-reference"
---

# @hy-sde-org/dsh-tool-graph

English | [中文](README.zh.md)

## Summary

`dsh-tool-graph` is the model-facing port slice P4 of the Agent Graph (Maka `stream-graph-supervisor-tools`). It contributes exactly three tools — `view_agent_graph`, `update_agent_graph`, `yield_agent_graph` — plus the `orchestration:graph` prompt section, and the host-side controller they run on. The tools are root-only and direct-only: only the graph's root session may call them, and they address the graph by id instead of delegating work generation to the model.

The host composes the storage (P1) and derivation (P2) packages, constructs one `AgentGraphController`, and provides it under the `agentGraphController` service. The plugin resolves that service with `ctx.get`; when it is absent the tools still mount (an agent preset never breaks session creation over an optional host assembly) and every graph tool call fails loud with `[agent-graph-unavailable]` until the host provides the controller. All durable decisions flow through the controller's coordinator, so a tool call commits exactly one store update and never re-runs a provider.

Every model-visible list is bounded with explicit `omitted` counts, and every `update_agent_graph` payload is cleaned by Maka's discriminator-tolerant preprocessors before it reaches the store. Source-triple idempotency makes a replayed or retried call a no-op: the same graph/session/key payload commits once and returns the existing row.

## Table of Contents

- [Use this package](#use-this-package)
- [Understand the implementation](#understand-the-implementation)
- [Further Exploration](#further-exploration)
- [Model Experience](#model-experience)
- [Known Limitations and Deferred Work](#known-limitations-and-deferred-work)

## Use this package

```ts
import toolGraph, { createAgentGraphController } from '@hy-sde-org/dsh-tool-graph'
import { GraphControlStore } from '@hy-sde-org/dsh-graph-control'

const store = await GraphControlStore.open(unit)
const controller = createAgentGraphController({
  store,
  rootSessionId: session.id,
  newId,
  options: { executor, recordSource, maxNewActivations: 4 },
})
ctx.provide('agentGraphController', controller)
await ctx.plugin(toolGraph, {})
```

Construct exactly one controller per graph root session. The plugin's `apply` registers the three tools and the prompt section; composing the seed session's agent with the preset that mounts `tool-graph` makes the tools resolve.

## Understand the implementation

- **Root-only enforcement**: DSH has no `direct_only`/`nesting` tool flag, so the guard lives in the tool body — `call.sessionId` must equal the controller's `rootSessionId`, otherwise `not_root_session`. The identity comes from the live execution context (`agent.session.id` plus the `turnBoundary` projection's `lastTurn`).
- **Run/turn mapping**: DSH has no Maka run list, so one tool call maps to the agent-loop turn boundary: `runId = graph_run_<n>` and `turnId = graph_turn_<n>` from `lastTurn`, with `toolCallId` from the call id. The mapping is documented, not claimed to be Maka-identical.
- **Idempotency**: `graphUpdateId` hashes `(graphId, sessionId, runId, turnId, toolCallId)`; an explicit `idempotencyKey` replaces all three of run/turn/call so a retried identical update in any later turn commits once (`created: false`, same revision, one store row).
- **Preprocessors**: `cleanUpdateInput`/`cleanAddWorkInput` pick fields by discriminator — `targetKind` keeps exactly one of `agentId`/`subagentId`/`operatorId`, `replacementMode: 'none'` drops `replaces`, instructions are trimmed. Bounds match Maka: 32 addWork items, 64 input ids, 64 selected results, 60 000 instruction chars, 20 stop targets, 64 finish result ids, 4000 reason chars.
- **View bounding**: live (requested) work pages through an opaque `work:<id>` cursor (64 per page); terminal work, stopped targets, records (truncated summaries), and readiness intents are tailed to 64 each with explicit `omitted` counts. `view_agent_graph` is total: an unknown graph returns an empty snapshot.
- **Wake semantics**: the tools never poll — `yield_agent_graph` calls `claimSupervisorWake` when requested work, live claims, or readiness intents exist, and returns `nothing_to_yield` otherwise. The host drives reconciliation and wakes the root session from the durable wake row.

## Further Exploration

- [`port_maka.md`](../../../../workspace/port_maka.md) — the port's design note and phase checklist.
- Maka reference: `packages/runtime/src/stream-graph-supervisor-tools.ts` in the Maka checkout.

## Model Experience

The model sees three tools and one prompt section (nothing else in this package is model-visible):

- `view_agent_graph` — bounded snapshot of one graph: work statuses, truncated record summaries, readiness intents, `omitted` counts, and an opaque `nextCursor` for paging live state.
- `update_agent_graph` — one durable decision per call: add work (one identity field per item via `targetKind`, `replaces` an existing work id), stop targets, or finish the graph with committed result ids. `idempotencyKey` makes retries safe.
- `yield_agent_graph` — ends the supervisor turn cooperatively while graph work continues; the host wakes the root session at the next durable checkpoint.
- `orchestration:graph` prompt section — instructs the supervisor to yield instead of poll, report outcomes per wave, and never invent work ids.

## Known Limitations and Deferred Work

- No graph registry: the store has no create/list graph operations, so `view_agent_graph` on an unknown graph returns an empty snapshot rather than `unknown_graph` (the error code exists for a later registry slice).
- Completed work stays `requested` (P2 status model has no terminal work state), so `yield_agent_graph.pendingWorkCount` counts requested schedule rows — activity (claims/intents) is reflected by the wake gate, not the count.
- The tools accept an explicit `workId` per addWork item (an extension over Maka) so an update can reference its own new work deterministically; deterministically derived ids remain the default.
- The plugin is exercised through a hand-built test composition; a Loader-booted cordis.yml composition test (packages/AGENTS.md product-plugin policy) is deferred to the integration slice. The opt-in composition patch at [`apps/cli/config/examples/graph/cordis.yml`](../../../apps/cli/config/examples/graph/cordis.yml) shows the intended mount: the host row provides the controller and this package mounts as a preset row of the graph root session.
