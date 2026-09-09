---
description: "Host-side wake delivery for the Agent Graph: idle-gated delivery of durable supervisor wakes over the P1 control store."
kind: "package-reference"
---

# @hy-sde-org/dsh-graph-wakes

English | [中文](README.zh.md)

## Summary

`dsh-graph-wakes` is the delivery half of the Agent Graph supervisor wake path (Maka port, slice P5). `dsh-graph-control` (P1) already owns the durable wake rows (`pending | running | waiting_permission | delivered | superseded | retryable_failed`); this package supplies the process-local runtime that carries a due wake into its owning root session **at the next idle boundary** — never while a turn is running — and settles every attempt durably through the store's own begin/complete CAS.

The runtime is deliberately decoupled. `GraphWakeRuntime` takes a store seam (`GraphControlStore` satisfies it structurally), a `deliver` hook that receives `{ graphId, wakeId, rootSessionId, snapshotVersion }` (P6 wiring re-drives the `AgentGraphCoordinator` and enqueues the supervisor checkpoint — the runtime never imports the coordinator), an optional `onCompact(sessionId)` compaction hook, and an injectable idle observer. Delivery is idle-gated by construction: the only delivery entry point is `handleIdle()`, and the injected observer forwards `agent/status === 'idle'` boundaries to it — the same status-observation seam the Schedule package uses — so the runtime starts no delivery on its own.

This package contributes no tool, prompt, or plugin row — the host wiring of P6 mounts the runtime and supplies the observer and deliver hook.

## Table of Contents

- [Use this package](#use-this-package)
- [Understand the implementation](#understand-the-implementation)
- [Further Exploration](#further-exploration)
- [Model Experience](#model-experience)
- [Known Limitations and Deferred Work](#known-limitations-and-deferred-work)

## Use this package

```ts
import { GraphWakeRuntime } from '@hy-sde-org/dsh-graph-wakes'

const runtime = new GraphWakeRuntime({
  store, // GraphControlStore (structural seam)
  deliver: async ({ graphId, wakeId, rootSessionId, snapshotVersion }) => {
    await coordinator.wake() // P6: re-drive the graph, enqueue the checkpoint
    return { kind: 'delivered' }
  },
  onCompact: sessionId => ctx.compaction.compactIfNeeded({ session }, 'context-overflow', signal).then(() => {}),
  observeIdle: onIdle => {
    return ctx.on('agent/status', ({ agent, status }) => {
      if (status === 'idle') onIdle(agent.id)
    })
  },
})

runtime.start('session-root') // scope: one root; observe every root when omitted
await runtime.handleIdle('session-root') // the delivery entry point (and test hook)
await runtime.stop() // unsubscribe, cancel timers, await the in-flight sweep
```

```ts
// Accessors
await runtime.pendingWakes('graph_g1') // pending + retryable wakes (terminal exhausted included)
await runtime.wakeStatus('graph_wake_abc') // durable row, any status
```

## Understand the implementation

### Idle-gated delivery

Delivery runs only from `handleIdle(sessionId?)`. `start(rootSessionId?)` registers the injected `observeIdle` observer, which production wiring backs with `agent.ctx.on('agent/status', …)` guarded to `status === 'idle'` exactly like the Schedule plugin; the observer merely forwards to `handleIdle`. Idle signals fire between turns, so a wake is never delivered while a turn is running; the deliver hook itself must run the wake through the owning agent's maintenance/idle seam (P6), mirroring how `ScheduleRuntime` claims the idle phase with `runMaintenance` before `followup()`. `start` never delivers on its own. The observer reports only live roots, so enumeration is naturally scoped to sessions that can actually receive a turn; the re-arm timer is only a re-drive hint and still enters through `handleIdle`.

### Delivery attempts and the durable state machine

For every due wake the runtime calls the store's `beginSupervisorWakeAttempt` with a deterministic attempt id (`graphWakeAttemptId(wakeId, attemptIndex)`, `turnId` same value): the store increments `attemptCount` and moves the wake `pending → running` atomically under its write chain, refusing once the row is delivered or superseded. The deliver hook result is then settled through `completeSupervisorWakeAttempt`:

| deliver outcome | durable attempt status | re-armed? |
| --- | --- | --- |
| `delivered` | `delivered` | no |
| `waiting_permission` | `waiting_permission` | no (parked until host resumption) |
| `superseded` / `stopped` | `superseded` | no |
| `retryable_failed` | `retryable_failed` | yes, unless exhausted |

### Retries, backoff, and terminal failure

A `retryable_failed` outcome re-arms the wake at `outcome.nextAttemptAt` or `now + 30 s × attemptNumber` (defaults: `DEFAULT_RETRY_BACKOFF_MS`, `DEFAULT_MAX_DELIVERY_ATTEMPTS = 3`); the re-arm is process-local and a segmented timer re-drives `handleIdle` for the owning root. Once `attemptCount` reaches `maxAttempts` the wake is terminal for this runtime — the store has no `failed` status, so the row stays `retryable_failed` at the cap and is never re-armed again.

### Context-overflow recovery

When a delivery returns `retryable_failed` with `overflow: true`, the runtime calls `onCompact(rootSessionId)` at most once per wake, then re-arms immediately for one bounded partial delivery (the deliver hook decides the partial shape and reports it with `partialResult: true`). If the partial attempt itself overflows, or the overflow arrives after the compact without declaring a partial, the runtime refuses a third identical full delivery: the wake is terminal (`exhausted` recovery state). With no `onCompact` wired, an overflowing wake terminates after the first attempt. The P1 attempt row has no `partialResult` column, so these markers stay process-local (see limitations).

### Stop suppression

A due wake is superseded **without delivery** when the graph's schedule log says the graph is stopped or closed: any update with a `finish` (graph closed — the coordinator's `isClosed` authority), or a log stop whose `targetId` is the root session or the graph id. The latter is the graph-level stop convention: this slice gives one session one graph, so a graph stop is recorded as a stop targeting the root identity, while work-item stops (work ids) never cancel wakes. The runtime mirrors the Schedule package's stop-cancels-due-record behavior by folding the durable log at every sweep instead of trusting process state.

### Single-flight and idempotency

Overlapping idle signals coalesce into one serial sweep (single-flight, re-requested when a signal lands during settlement), and the store CAS makes one attempt row one delivery: a second runtime or a retried sweep that begins the same attempt id observes `acquired: false` and delivers nothing. Store failures surface through `onError` and leave the attempt row `running` for host recovery; a throwing deliver hook settles as `retryable_failed` with the hook's message.

### Restart durability

All state the runtime needs is in the store: a fresh `GraphWakeRuntime` over the same store sees the same wake rows, re-arms orphaned `retryable_failed` wakes at the next idle (like Maka's `recover`), and refuses nothing that the attempt count still allows. Backoff timestamps and overflow markers are process-local and are deliberately not persisted.

## Further Exploration

- `packages/graph/graph-control` (P1): the durable wake rows and the begin/complete CAS this runtime settles.
- `packages/graph/graph-stream` (P2): `AgentGraphCoordinator`, re-driven by the P6 deliver hook.
- `packages/schedule/schedule` — the observation pattern (`agent/status === 'idle'` + `agent.whenIdle`) this runtime mirrors.
- Maka reference: `packages/runtime/src/agent-graph-supervisor-wake.ts` in the Maka checkout (authoritative wake semantics).
- `tests/graph-wakes.spec.ts`: real sqlite-backed store with a fake idle observer and deliver hook.

## Model Experience

No model-facing surface. This package is host-side machinery; the supervisor tools of slice P4 are what the model sees, and P6 wiring turns `deliver` into the model-visible checkpoint turn.

## Known Limitations and Deferred Work

- The P1 attempt row has no `partialResult` (or overflow) column: the one-compact/one-partial markers are process-local. A restart clears them, so one more full attempt can occur before the `attemptCount` cap stops retries; exceeding the cap is still impossible.
- Terminal failure has no store status: exhausted wakes stay `retryable_failed` at the attempt cap. P1's status union is fixed in this slice.
- `running` wakes (crash between begin and complete) are not recovered here: whether an interrupted attempt really completed is a runtime fact, and the P1 `recoverSupervisorWakes` no-op keeps that fact with host wiring (P6).
- `waiting_permission` wakes are parked and never re-attempted; permission-response resumption (Maka `notifyPermissionResponse`) is deferred to P6.
- Cross-process coordination is out of scope: like the coordinator, the runtime is process-local, so another process holding the same store does not wake this runtime.
- The graph-level stop convention (log stop with `targetId` equal to the root/graph id) is defined here; a work-item stop never suppresses wakes. P4 must commit graph stops with the root identity for suppression to engage.
