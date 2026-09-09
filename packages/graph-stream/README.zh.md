---
description: "Agent Graph 的派生流层：确定性标识、记录/轨迹/就绪/调度投影、交接文本，以及进程内协调驱动器。"
kind: "package-reference"
---

# @hy-sde-org/dsh-graph-stream

[English](README.md) | 中文

## 摘要

`dsh-graph-stream` 是 Agent Graph 的派生层（Maka 移植，P2 切片）。它叠加在 `@hy-sde-org/dsh-graph-control`（持久化决策存储，P1）之上，负责**一切可以从已提交行重算出来的东西**：工作状态投影、记录折叠、轨迹/路由派生、就绪意图、输入交接文本，以及单飞式协调驱动器（`AgentGraphCoordinator`）——后者对该存储执行 Maka 原始驱动循环（预置 → 监督 → 选择 → 渲染 → 执行）。

拆分遵循 Maka 的一条硬规则：存储是权威，流层除经由存储自身的提交/声明/预置方法外绝不写任何东西。这里的每个投影都是确定性的纯函数——重算它不会启动任何工作；准予与执行由存储封口（在其观察到的调度修订号上按预分配 turn/run 身份声明）。

标识为确定性 sha256 并截取前 32 个十六进制字符（`graph_intent_…`、`graph_operator_…`、`graph_edge_…`、`graph_route_…`、`graph_record_…`、`graph_claim_…`），因此重放在构造上即幂等。身份比较使用 UTF-16 码元顺序（`compareAgentGraphIdentity`），而非 locale 比较。

本包不提供任何工具、提示词或插件行——由执行器适配器（P3）和主管工具（P4）消费。

## 目录

- [使用本包](#use-this-package)
- [理解实现](#understand-the-implementation)
- [进一步探索](#further-exploration)
- [模型体验](#model-experience)
- [已知限制与后续工作](#known-limitations-and-deferred-work)

<a id="use-this-package"></a>
## 使用本包

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

<a id="understand-the-implementation"></a>
## 理解实现

### 确定性标识与排序

`stableHash(value)` = `sha256:` + 规范化 JSON 的十六进制（对象键排序、`undefined`/函数/符号 → `"[undefined]"`、bigint → 数字字符串、`required`/`enum` 数组以 `localeCompare` 排序、Date → ISO）。所有标识取前 32 个十六进制字符。`compareAgentGraphIdentity` 使用 UTF-16 码元顺序——跨进程稳定，与 `localeCompare` 不同。

### 投影

- **调度投影**（`projectAgentGraphSchedule`）：把追加式更新日志折叠为模型可见的工作视图；校验修订号从 1 连续、finish 之后无更新、不重复 work id、图 id 不匹配即拒绝。`stopped` 优先于 `superseded`。
- **记录折叠**（`readCommittedAgentGraphProjection`）：按（操作员，会话）从已提交事件派生只引用记录；跳过 partial；每个激活至多一个 terminal；顺序确定。
- **轨迹**（`validateAgentGraphTraceTopology`、`buildAgentGraphTraceSnapshot`）：校验 DAG（重复 id/端点、自环、未知操作员、Kahn 判环），并为每条（记录 × 出边）派生一条路由。
- **就绪**（`buildAgentGraphReadinessSnapshot`）：map 策略——每条经声明的入边到达的路由产生一个意图，恰好密封触发它的记录（`policyFingerprint`、`readinessContextFingerprint`、稳定哈希生成 `graph_intent_…`）。
- **交接**（`hydrateAgentGraphInputHandoffs`、`renderAgentGraphScheduledWorkPrompt`）：解析有界结论文本（每条记录 16 KiB、总计 48 KiB，`…` 省略号，按码点二分），渲染操作员提示词：指令 + `GRAPH_OPERATOR_HANDOFF_PROTOCOL` + `<agent_graph_input_handoffs>`，`<` 转义为 `\u003c`。记录保持只引用；文本仅在渲染时解析。

### 协调与协调器

`reconcileAgentGraphSchedule` 执行 Maka 的各阶段：A 预置操作员 → B 派生主管意图 → C 选择（现有声明总会派发；新意图受 `maxNewActivations` 上限，超出 → `activation_limit`）→ D 渲染（全有或全无）→ E 执行（按修订号声明 → 运行时用 `admitExecution` 即按修订号开始执行）。状态：`reconciled | waiting | limit_reached | failed | cancelled | stale`。`applyScheduleStops` 执行替换（`status: 'superseded'`）、取消声明并按会话批量停止。

`AgentGraphCoordinator` 是进程内单飞驱动器：`scheduleUpdate` 提交一行后唤醒驱动器；`reconcileAndWait` 加入恰好一轮；`recover` 在重启后恢复非空调度的图；`stop`/`wake`/`isClosed` 暴露相同生命周期。只要还有工作或停止目标，一轮驱动就会再次运行；已有声明会被观察为已派发（执行器按声明 id 去重）。

### 执行器接缝

```ts
export interface AgentGraphExecutor {
  provisionOperator(request: AgentGraphOperatorProvisionRequest): Promise<AgentGraphOperatorProvisionResult | undefined>
  runClaimedAgentGraphIntent(input: AgentGraphRunClaimedIntentInput): Promise<void>
  stopSession(sessionId: string, opts?: { reason?: string }): Promise<void>
}
```

协调器从不直接调用提供方——P3 提供基于子代理与工作树的实现。

<a id="further-exploration"></a>
## 进一步探索

- `packages/graph/graph-control`（P1）：本层折叠的持久化行。
- `src/reconcile.ts` / `src/coordinator.ts`：驱动循环与状态推导。
- `src/hash.ts` / `src/identity.ts`：所有标识与指纹依赖的规范化与排序原语。
- `tests/graph-stream.spec.ts` / `tests/reconcile.spec.ts`：针对真实 sqlite 存储的投影、校验、交接与端到端驱动场景。

<a id="model-experience"></a>
## 模型体验

包为纯 TypeScript，模块单一职责、无环境状态。类型显式；结果为纯数据（除协调器外无跨模块类实例）。校验类错误带 `reason` 码；存储的冲突错误原样上抛。

<a id="known-limitations-and-deferred-work"></a>
## 已知限制与后续工作

- 就绪策略种类：仅 `map`——`all_settled` 与主管就绪种类推迟到 P4。
- 尚无客户端投影/检查点（`onCheckpoint`）；无工具视图分页；驻留为空操作。
- map 策略意图只派生、不由 reconcile 自动派发——它们留给 P4 的主管工具。
- 记录形态为带来源的副本（精简），是有意偏离 Maka 的 18 面完整记录；流层绝不修改已存记录。
- 协调器为进程本地：另一进程持有同一图存储不会自动唤醒本驱动器（唤醒投递为 P5）。
