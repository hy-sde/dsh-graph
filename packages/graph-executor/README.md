---
description: "Operator executor adapter for the Agent Graph: deterministic worktree leases, durable operator bindings, and serialized child runs that settle into terminal records."
kind: "package-reference"
---

# @hy-sde-org/dsh-graph-executor

English | [中文](README.zh.md)

## Summary

`dsh-graph-executor` is the child-operator executor adapter of the Agent Graph (Maka port, slice P3). It implements the coordinator's `AgentGraphExecutor` surface over two injected seams: a `GraphOperatorWorktreePool` (acquire/release worktree leases for one repository) and a `GraphOperatorChildRunner` (one provider-backed child run per activation). The control store stays the durable authority — provisions and bindings are rows, and everything else is derived.

Provisioning is deterministic and idempotent: `provisionOperator` derives `graph_operator_lease_<sha256(graphId, workId, provisionFingerprint)[32]>` from the provision, so a retry adopts the same lease key, and `pool.acquire` is idempotent per key (process-local by design; the binding row is the durable hint a real pool consults after a restart). When no lease can be acquired the method returns `undefined` and the reconciler defers the work to a later drive.

Execution is serialized per operator (one child run at a time) and settles into exactly one terminal record per activation: the runner's summary is truncated to 16 KiB (`truncateUtf8`, `…` suffix) and emitted as an `AgentGraphRecordSourceEvent` (`facets: ['message', 'terminal']`) through `recordSink`; a failed run without a summary emits `[operator failed] <message>`. A child failure never throws from `runClaimedAgentGraphIntent` — records are the observation channel.

This package contributes no tool, prompt, or plugin row — the coordinator (P2) and supervisor tools (P4) consume it.

## Table of Contents

- [Use this package](#use-this-package)
- [Understand the implementation](#understand-the-implementation)
- [Further Exploration](#further-exploration)
- [Model Experience](#model-experience)
- [Known Limitations and Deferred Work](#known-limitations-and-deferred-work)

## Use this package

```ts
import { GraphControlStore } from '@hy-sde-org/dsh-graph-control'
import { AgentGraphCoordinator } from '@hy-sde-org/dsh-graph-stream'
import { createGraphOperatorExecutor } from '@hy-sde-org/dsh-graph-executor'

const executor = createGraphOperatorExecutor({
  store,
  pool: worktreePool,       // GraphOperatorWorktreePool over the git worktree engine
  childRunner: childRunner, // GraphOperatorChildRunner over the subagent runtime
  recordSink: recordSink,   // commits AgentGraphRecordSourceEvent rows
  newId,
})

const coordinator = new AgentGraphCoordinator(graphId, {
  store,
  executor,
  recordSource,
  newId,
  maxNewActivations: 4,
})
```

## Understand the implementation

### Provision: lease key, binding, and the durable row

`provisionKey(request)` hashes `{ graphId, workId, provisionFingerprint }` with `stableHash32`, so retries of the same provision reuse one lease key. `provisionOperator` then acquires the lease (or adopts it), persists `bindOperatorWorktree({ graphId, workId, provisionId, leaseId, path, repoRoot, boundAt })`, and finally commits the provision row through the store — the same revision-conditional, closure-blocked write the P1 store already owns. Acquire failure returns `undefined`; the binding row is never written without a lease. Re-binding the same provision with a different lease id is rejected (`binding-conflict`), so a provision owns exactly one worktree.

### Run: admission, serialization, and settlement

`runClaimedAgentGraphIntent` queues the activation on a per-operator promise chain, so one operator never runs two children at once. Inside the queue it first evaluates `admitExecution` (post-serialization revision gate) and aborts when it returns `cancelled`; then it resolves the operator's binding — through the `${graphId}:${workId}` index when the intent's readiness id names the provisioning work (dynamic operators), or through the provision's `operatorId` for operator-targeted work, which re-runs a provisioned operator — and starts the child with `{ sessionId, instructions, workspace: binding.path, runId, labels, abortSignal }`. `concurrencyHint` optionally caps concurrently running child starts across operators.

After the child settles, one `AgentGraphRecordSourceEvent` is built (`runtimeEventId` from `newId`, `seq` 1, the claim's `targetRunId`, truncated summary) and handed to `recordSink`; the folded `AgentGraphRecord` is also returned. `recordSink` failure propagates — the terminal record is the commit point that stops the reconciler from re-dispatching the same claim.

### Bindings in the control store

The P1 store gained one authoritative table, `operator_bindings` (row key `provisionId`), with a derived `${graphId}:${workId}` index rebuilt at open. Methods: `bindOperatorWorktree` (idempotent same-lease rebind keeps the original `boundAt`; different-lease rebind throws `binding-conflict`), `readOperatorBinding`, `readOperatorBindingByWork`, `listOperatorBindings(graphId?)`.

## Further Exploration

- `packages/graph/graph-control` (P1): the store rows (provisions, bindings) and their durability contract.
- `packages/graph/graph-stream` (P2): `AgentGraphExecutor`, the record fold (`readCommittedAgentGraphProjection`), `stableHash32`, `truncateUtf8`.
- Maka reference: `packages/storage/src/git-worktree-child-executor.ts` (lease identity, deterministic paths, worktrees surviving terminal runs) and `session-manager.ts` (`runClaimedAgentGraphIntent`).
- `tests/graph-executor.spec.ts`: fake pool/runner plus a real sqlite-backed store.

## Model Experience

No model-facing surface. The package is host-side machinery: it renders no prompt (P2 does), and the terminal summaries it records are what the P4 supervisor tools present.

## Known Limitations and Deferred Work

- Lease idempotency is process-local: a real pool implementation adopts a previously leased worktree after a restart by matching `listWorktrees`; nothing here re-derives leases from the store binding (the binding is the durable hint, not the lease authority).
- Worktrees intentionally survive terminal runs (Maka contract); the executor never releases a lease. Pool release is wired in for graph teardown in a later slice.
- `recordSink` failures propagate as execution failures — the reconciler reports them and the host retries; there is no crash-consistent terminal-event log in this slice.
- `runClaimedAgentGraphIntent` observes the seam contract the P2 README documents (`provisionOperator` may return `undefined`); the P2 `AgentGraphExecutor` type in `packages/graph/graph-stream/src/types.ts` still declares a non-optional provision result and is expected to line up on integration.
