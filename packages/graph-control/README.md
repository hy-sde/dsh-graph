---
description: "Durable control plane for the Agent Graph: schedule updates, exactly-once intent claims, operator provisions, and supervisor wakes."
kind: "package-reference"
---

# @hy-sde-org/dsh-graph-control

English | [中文](README.zh.md)

## Summary

`dsh-graph-control` is the durable decision store for the Agent Graph (Maka port, slice P1). It owns exactly the stateful rows a graph needs — the schedule-update log, exactly-once intent claims, operator provisions, and supervisor wakes — and nothing else: records, routes, readiness intents, work status, and client snapshots stay derived (session-projection folds) in later slices.

The store is designed around Maka's one hard rule: **the durable claim (with preallocated turn/run identity) is written before the runtime is ever asked to run**, and every claim/provision transition is conditional on the schedule revision it observed. A retry therefore reuses the same activation identity instead of invoking the provider twice. All ids are deterministic sha256 (`graph_update_…`, `graph_claim_…`, `graph_operator_…`, `graph_wake_…`), so replays are idempotent by construction.

Persistence: one `KvUnit` (`name: agent_graph`) with five authoritative tables; derived uniqueness indexes are rebuilt from those rows at open, so a torn write heals instead of corrupting. The storage contract forbids concurrent writers on one unit, so the store serializes mutations on one write chain and each per-record write is durable.

This package contributes no tool, prompt, or plugin row — the coordinator (P2), executor adapter (P3), and supervisor tools (P4) consume it.

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

const backend = await ctx[storageBackendServiceKey('sqlite')]
const unit = await backend.kv.open(GraphControlStore.descriptor)
const store = await GraphControlStore.open(unit)

const { update, created } = await store.commitScheduleUpdate(request)
const { claim } = await store.claimIntentAtScheduleRevision(claimRequest, update.revision)
```

Open the unit exactly once per process: the storage layer rejects double-open, and the store is the single writer chain over the unit.

## Understand the implementation

- **Schedule log** (`schedule`): append-only decisions, revision = max+1, idempotent by `updateId` and by source triple `(session, run, toolCall)`; `finish` cannot combine with `add_work`; the graph is closed once a finish is committed.
- **Intent claims** (`claims`): keyed `graphId:intentId`, with activation-identity uniqueness (`(targetSessionId, targetTurnId)` and `(targetSessionId, targetRunId)`) enforced against derived indexes; transitions `claimed → executing → cancelled` are revision-conditional; fresh claims are rejected after closure while existing claims stay dispatchable.
- **Operator provisions** (`provisions`): deterministic `provisionId`/`operatorId` make retries adopt the same operator; revision-conditional and closure-blocked like claims.
- **Supervisor wakes** (`wakes` + `wake_attempts`): claim once, begin attempts (refused once delivered/superseded), complete with `waiting_permission | delivered | superseded | retryable_failed`; supersede by root session (+ optional graph filter); `recoverSupervisorWakes()` is deliberately a no-op — whether an interrupted attempt really completed is a Runtime fact, so the coordinator (P5) inspects run facts and completes accordingly. The store never guesses.

## Further Exploration

- [`port_maka.md`](../../../../workspace/port_maka.md) — the port's design note and phase checklist.
- Maka reference: `docs/architecture/agent-graph-stream-scheduling-draft.md` (Chapter 7) in the Maka checkout.

## Model Experience

No model-facing surface. This package is host-side machinery; the supervisor tools of slice P4 are what the model sees.

## Known Limitations and Deferred Work

- No epoch table: one DSH session owns one graph per the design decision (multi-graph-per-root is deferred).
- Multi-row CAS is process-atomic (one write chain), not transaction-atomic; a crash mid-sequence heals on open because indexes are derived. Claims with a torn write are recovered by the coordinator inspecting run facts, as in Maka.
- Derived work status (`requested/stopped/superseded`), records, routes, readiness, and client snapshots belong to later slices and are not stored here.
