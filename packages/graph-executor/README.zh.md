---
description: "Agent Graph 的子操作员执行器适配器：确定性工作树租约、持久化操作员绑定，以及以终结记录收尾的串行子运行。"
kind: "package-reference"
---

# @hy-sde-org/dsh-graph-executor

[English](README.md) | 中文

## 摘要

`dsh-graph-executor` 是 Agent Graph 的子操作员执行器适配器（Maka 移植，P3 切片）。它基于两条注入接缝实现协调器的 `AgentGraphExecutor` 表面：`GraphOperatorWorktreePool`（为同一仓库获取/释放工作树租约）与 `GraphOperatorChildRunner`（每个激活一次提供方支撑的子运行）。控制存储仍是持久化权威——预置与绑定都是行，其余一切皆为派生。

预置是确定且幂等的：`provisionOperator` 从预置导出 `graph_operator_lease_<sha256(graphId, workId, provisionFingerprint)[32]>`，因此重试采用同一租约键，且 `pool.acquire` 对每个键幂等（按设计为进程本地；绑定行是真实池在重启后参考的持久化提示）。无法获取租约时返回 `undefined`，协调器将工作推迟到后续一轮驱动。

执行按操作员串行（同一时刻只有一个子运行），并以每个激活恰好一条终结记录收尾：运行器的摘要被截断到 16 KiB（`truncateUtf8`，`…` 后缀），并通过 `recordSink` 以 `AgentGraphRecordSourceEvent`（`facets: ['message', 'terminal']`）发出；无摘要的失败运行发出 `[operator failed] <message>`。子运行失败绝不会从 `runClaimedAgentGraphIntent` 抛出——记录就是观察通道。

本包不提供任何工具、提示词或插件行——由协调器（P2）与主管工具（P4）消费。

## 目录

- [使用本包](#use-this-package)
- [理解实现](#understand-the-implementation)
- [进一步探索](#further-exploration)
- [模型体验](#model-experience)
- [已知限制与后续工作](#known-limitations-and-deferred-work)

<a id="use-this-package"></a>
## 使用本包

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

<a id="understand-the-implementation"></a>
## 理解实现

### 预置：租约键、绑定与持久化行

`provisionKey(request)` 用 `stableHash32` 哈希 `{ graphId, workId, provisionFingerprint }`，因此同一预置的重试复用一个租约键。随后 `provisionOperator` 获取（或采纳）租约，持久化 `bindOperatorWorktree({ graphId, workId, provisionId, leaseId, path, repoRoot, boundAt })`，最后通过存储提交预置行——与 P1 存储已拥有的修订条件、闭包阻断写入相同。获取失败返回 `undefined`；无租约绝不写入绑定行。以不同租约 id 重新绑定同一预置会被拒绝（`binding-conflict`），因此一个预置恰好拥有一个工作树。

### 运行：准入、串行化与结算

`runClaimedAgentGraphIntent` 把激活排入按操作员划分的 promise 链，因此一个操作员绝不会同时运行两个子任务。在队列内它先评估 `admitExecution`（序列化后的修订门禁），返回 `cancelled` 时中止；随后解析操作员的绑定——当意图的就绪 id 指向预置工作（动态操作员）时走 `${graphId}:${workId}` 索引，面向已有操作员的工作（重跑一个已预置操作员）则通过预置的 `operatorId` 找到——并以 `{ sessionId, instructions, workspace: binding.path, runId, labels, abortSignal }` 启动子任务。`concurrencyHint` 可选地限制跨操作员并发运行的子任务数。

子任务结算后，构造一条 `AgentGraphRecordSourceEvent`（`runtimeEventId` 来自 `newId`，`seq` 为 1，声明的 `targetRunId`，截断后的摘要）交给 `recordSink`；折叠出的 `AgentGraphRecord` 同时返回。`recordSink` 失败会向上传播——终结记录是阻止协调器重新派发同一声明的提交点。

### 控制存储中的绑定

P1 存储新增一张权威表 `operator_bindings`（行键 `provisionId`），并带有一个在打开时重建的派生索引 `${graphId}:${workId}`。方法：`bindOperatorWorktree`（同租约幂等重绑保留原始 `boundAt`；异租约重绑抛 `binding-conflict`）、`readOperatorBinding`、`readOperatorBindingByWork`、`listOperatorBindings(graphId?)`。

<a id="further-exploration"></a>
## 进一步探索

- `packages/graph/graph-control`（P1）：存储行（预置、绑定）及其持久化契约。
- `packages/graph/graph-stream`（P2）：`AgentGraphExecutor`、记录折叠（`readCommittedAgentGraphProjection`）、`stableHash32`、`truncateUtf8`。
- Maka 参考：`packages/storage/src/git-worktree-child-executor.ts`（租约身份、确定性路径、工作树跨终结运行存活）与 `session-manager.ts`（`runClaimedAgentGraphIntent`）。
- `tests/graph-executor.spec.ts`：假池/运行器加真实 sqlite 存储。

<a id="model-experience"></a>
## 模型体验

无面向模型的表面。包是宿主侧机制：不渲染任何提示词（由 P2 负责），它记录的终结摘要由 P4 主管工具呈现。

<a id="known-limitations-and-deferred-work"></a>
## 已知限制与后续工作

- 租约幂等性为进程本地：真实池实现通过在重启后匹配 `listWorktrees` 采纳先前已租用的工作树；此处不从存储绑定重新推导租约（绑定是持久化提示，不是租约权威）。
- 工作树按设计在终结运行后存活（Maka 契约）；执行器绝不释放租约。池释放为后续切片中的图拆除而接入。
- `recordSink` 失败作为执行失败传播——协调器上报并重试；本切片没有崩溃一致的终结事件日志。
- `runClaimedAgentGraphIntent` 遵循 P2 README 记录的接缝契约（`provisionOperator` 可能返回 `undefined`）；`packages/graph/graph-stream/src/types.ts` 中的 P2 `AgentGraphExecutor` 类型仍声明了非可选预置结果，期望在集成时对齐。
