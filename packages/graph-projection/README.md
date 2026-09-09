---
description: "Serves the session's standing agent-graph snapshot from the host's graph/change publishes for clients and maintainers composing or debugging the graph projection unit."
kind: "package-reference"
---

# @hy-sde-org/dsh-graph-projection

English | [中文](README.zh.md)

## Summary

`dsh-graph-projection` serves the session's standing agent-graph snapshot — the whole bounded `SessionGraphProjection` (graph identity, closed/active status, revision, bounded work list, omitted counts, pending wake) — as the `graph` projection unit. The host (P7) owns graph state and publishes a complete post-change snapshot per graph movement as a `graph/change` session event; this unit folds those publishes into the session-projection seam (registry snapshot, change feed, every projection carrier) with no store coupling. Choose it in compositions that already mount the projection registry, such as the web app bundle whose graph rail is the reference consumer; assemblies without the registry are unaffected and their consumers read no graph key. Setup and publish semantics come first; the fold internals live in a collapsible developer section below.

## Table of Contents

- [Use this package](#use-this-package)
- [Understand the implementation](#understand-the-implementation)
- [Further Exploration](#further-exploration)
- [Model Experience](#model-experience)
- [Known Limitations and Deferred Work](#known-limitations-and-deferred-work)
- [Dev Note](#dev-note)

-----

<a id="use-this-package"></a>
## Use this package

Mount the plugin beside the session store and the projection registry when clients should render the session's agent graph from whole published values without coupling to the graph control store. The unit registers only when the registry is present.

### Composition

```yaml
- name: '@deepseek-ai/dsh-session'
- name: '@deepseek-ai/dsh-session-projection'
- name: '@hy-sde-org/dsh-graph-projection'
```

### What the value means

| Field | Meaning |
|---|---|
| `schemaVersion` | Wire format version; `1` for this projection. |
| `graphId` | The graph the snapshot belongs to. |
| `status` | `active` while the graph runs, `closed` after a finish. |
| `revision` | Host-assigned publish revision, strictly increasing per graph; the fold gates stale re-publishes on it. |
| `closed` | Mirror of the terminal state for consumers that branch on it. |
| `work` | Bounded work list: `workId`, execution-lifetime `status` (`requested`/`claimed`/`executing`/`stopped`/`finished`/`failed`), bounded `instruction` (≤300 chars), optional `operatorId`, `inputCount`. |
| `omitted` | Rail-budget overflows the host capped away: work, records, and inputs counts. |
| `pendingWake` | Whether a supervisor wake is pending delivery to the root session. |
| `updatedAt` | Host publish timestamp. |

The wire value is a whole post-change snapshot (whole-value rule): consumers replace, never merge. The host appends one `graph/change` event per graph state change with `session.append('graph/change', graphSnapshotToEvent(graphId, snapshot, revision))` — the helper exported here builds the exact payload, keeping publish revision and snapshot revision aligned. Before the first publish the unit serves `null`: the session hosts no graph yet, and consumers treat it as not-yet-available rather than an empty graph.

<a id="graph-chip"></a>
### Graph chip

The web chat UI reads the value through the session-projection standard seat, `useProjection('graph')`; the typed key comes from a type-only import of `@hy-sde-org/dsh-graph-projection/types`, a client dev-dependency erased at runtime (no client-to-graph runtime edge). The transcript renders the chip while the projection is present and the rail has work (`work.length > 0`) or the graph is still `active`; an absent key, `null` before the first publish, or a closed graph with an empty rail renders nothing. The row reads `Continued by Agent Graph` with the summary `N work item(s) · active|closed` (`由 Agent Graph 继续` / `N 个工作项` in the Chinese UI), styled like the compaction chip. The chip is view-layer state; the chat snapshot never carries projection values.

### Failures and recovery

The unit is inert without the projection registry: `inject` keeps the fiber pending and nothing registers, so other assemblies lack the `graph` key. Unmounting the plugin removes the key, because registrations are effects on the mounting fiber. Persisted-cache rows are schema-validated on restore — including the version-positive revision and the ≤300-character instruction bound — so a corrupt row is discarded instead of seeding a broken fold.

-----

<a id="understand-the-implementation"></a>
## Understand the implementation

<details>
<summary>Implementation internals — click to expand</summary>

This section explains the fold behind the snapshot; the observable behavior is fully covered in [Use this package](#use-this-package).

### Design concept

The unit is a pure fold over committed `graph/change` events. One DSH session owns one graph, so the fold replaces its state only for the graph it already hosts: a publish naming a different `graphId` keeps the standing snapshot (reference preserved, so the registry's identity gate keeps the change feed quiet) while the watermark records the observed event — the session's graph never flips mid-life. A publish whose revision does not advance past the last folded one is a stale re-publish and returns the same state reference, so repeated deliveries push nothing. The wire view is the event payload's snapshot reference itself (identity), which is exactly why the feed stays quiet across internal-only changes: no clone, no re-render.

### Source map

| File | Role |
|---|---|
| [`src/index.ts`](src/index.ts) | Plugin entry: `inject`, unit registration on the mounting fiber |
| [`src/projection.ts`](src/projection.ts) | The fold: state schema, revision gate, wire view, publish helper, instruction bound |
| [`src/types.ts`](src/types.ts) | One home of the `graph` projection-key declaration, `graph/change` event augmentation, and wire types |
| — | No runtime invariant companion is published: the package owns one pure projection fold, `session-projection` schema-validates its served values, and re-folding the same log would duplicate the implementation instead of comparing independently maintained observations; graph-control owns the durable rows the host publishes from. |

### Fold rules

- Uninteresting events return the same state reference; the registry's two `Object.is` gates then hold the feed to actual snapshot changes only.
- The first publish adopts any graph id (no standing graph yet); every later publish for a different id is recorded in the watermark only.
- A stale (non-advancing) revision returns the same state; a matching-graph publish replaces snapshot, watermark, and revision atomically.
- The served value is the payload snapshot by reference — the projection performs no copy, truncation, or re-derivation; bounds are the host's publish contract, validated at the persisted-state boundary here.

</details>

-----

<a id="further-exploration"></a>
## Further Exploration

Read these pages when the unit's contract is not enough. They move from the registry that drives units to the graph packages the host publishes from.

- [Session projection subsystem](../../../docs/subsystems/session-projection.md) — the registry that drives units and serves snapshot and change-feed values.
- [Session projection registry package](../../session/session-projection/README.md) — the registry contract units register against.
- [Graph stream package](../graph-stream/README.md) — the derived stream layer whose schedule and records feed the host's published snapshots.
- [Tool graph package](../tool-graph/README.md) — the bounded model-visible snapshot vocabulary the client projection reuses with a tighter rail budget.

-----

<a id="model-experience"></a>
## Model Experience

None, as the `graph` unit folds already-published host snapshots into a client-facing read model and registers nothing model-facing.

#### KV Cache effect

None; the package never assembles or sends provider requests.

## Known Limitations and Deferred Work

<a id="known-limitations-and-deferred-work"></a>


These limits define what the projection serves and when the unit is absent. They are current package constraints.

- **The wire value carries the whole snapshot on every push** — ignored by the identity gate for internal-only changes, but a real re-publish delivers the complete work list (whole-value rule); splitting work into an on-demand read is deferred until sessions with many hundreds of work items need it.
- **One graph per session** — a foreign `graphId` publish never displaces the standing snapshot, so a session cannot host two graphs concurrently; multi-graph sessions would need a key per graph.
- **Status and bounds are host-trusted** — the projection validates the persisted shape and the ≤300-character instruction bound, but the live publish path trusts the host to assign execution-lifetime statuses and truncate instructions before appending.
- **`null` until the first publish** — mounting the plugin registers the key immediately, but the value stays `null` until the host publishes; consumers must handle the not-yet-available state.
- **Mounted only where the projection registry is composed** — other assemblies serve no `graph` key, and their consumers read the absence as no graph capability.

<a id="dev-note"></a>
### Dev Note

<details>
<summary>Working context for maintainers — click to expand</summary>

None.

</details>
