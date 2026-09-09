---
description: "Agent Graph 的主管可见面：三个仅根会话工具（查看/更新/让出）、主机侧图控制器与 orchestration:graph 提示词段落。"
kind: "package-reference"
---

# @hy-sde-org/dsh-tool-graph

[English](README.md) | 中文

## 摘要

`dsh-tool-graph` 是 Agent Graph 的模型可见移植切片 P4（Maka `stream-graph-supervisor-tools`）。它只贡献三个工具——`view_agent_graph`、`update_agent_graph`、`yield_agent_graph`——外加 `orchestration:graph` 提示词段落，以及它们所运行的主机侧控制器。这些工具仅限根会话且直接调用：只有图的根会话可以调用它们，它们按图 id 寻址，而不是把工作生成委托给模型。

主机组合存储（P1）与派生（P2）包，构造一个 `AgentGraphController`，并在 `agentGraphController` 服务下提供。本插件用 `ctx.get` 解析该服务；缺失时工具仍会挂载（agent 预设绝不因可选主机装配缺失而阻断会话创建），每次图工具调用都会以 `[agent-graph-unavailable]` 响亮失败，直到主机提供控制器。所有持久化决策都流经控制器的协调器，因此一次工具调用只提交一条存储更新，绝不重复运行提供方。

每个模型可见列表都有显式的 `omitted` 计数上限，每条 `update_agent_graph` 载荷在到达存储前都经 Maka 判别器容忍的预处理器清洗。源三元组幂等使重放或重试调用成为空操作：相同的图/会话/键载荷只提交一次，并返回既有行。

## 目录

- [使用本包](#use-this-package)
- [理解实现](#understand-the-implementation)
- [进一步探索](#further-exploration)
- [模型体验](#model-experience)
- [已知限制与后续工作](#known-limitations-and-deferred-work)

<a id="use-this-package"></a>
## 使用本包

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

每个图根会话只构造一个控制器。插件的 `apply` 注册三个工具与提示词段落；用挂载了 `tool-graph` 的预设组合种子会话的代理，即可让工具解析成功。

<a id="understand-the-implementation"></a>
## 理解实现

- **仅根会话强制**：DSH 没有 `direct_only`/`nesting` 工具标志，因此守卫位于工具体内——`call.sessionId` 必须等于控制器的 `rootSessionId`，否则返回 `not_root_session`。身份来自活动执行上下文（`agent.session.id` 加 `turnBoundary` 投影的 `lastTurn`）。
- **run/turn 映射**：DSH 没有 Maka 的运行列表，因此一次工具调用映射到代理循环的回合边界：`runId = graph_run_<n>`、`turnId = graph_turn_<n>`（取自 `lastTurn`），`toolCallId` 取自调用 id。该映射被如实记录，而非声称与 Maka 相同。
- **幂等**：`graphUpdateId` 对 `(graphId, sessionId, runId, turnId, toolCallId)` 做哈希；显式 `idempotencyKey` 会替换 run/turn/call 三者，使任意后续回合重试的相同更新只提交一次（`created: false`、修订号相同、存储一行）。
- **预处理器**：`cleanUpdateInput`/`cleanAddWorkInput` 按判别器挑选字段——`targetKind` 只保留 `agentId`/`subagentId`/`operatorId` 之一，`replacementMode: 'none'` 丢弃 `replaces`，指令做修剪。边界与 Maka 一致：32 个 addWork 项、64 个输入 id、64 个选中结果、60000 指令字符、20 个停止目标、64 个完成结果 id、4000 理由字符。
- **查看边界**：活动（requested）工作通过不透明 `work:<id>` 游标分页（每页 64）；终止工作、停止目标、记录（截断摘要）与就绪意图各尾部截取 64 条并带显式 `omitted` 计数。`view_agent_graph` 是全函数：未知图返回空快照。
- **唤醒语义**：工具从不轮询——当存在请求工作、活动声明或就绪意图时，`yield_agent_graph` 调用 `claimSupervisorWake`，否则返回 `nothing_to_yield`。主机驱动协调，并从持久化唤醒行唤醒根会话。

<a id="further-exploration"></a>
## 进一步探索

- [`port_maka.md`](../../../../workspace/port_maka.md) — 移植设计说明与阶段清单。
- Maka 参考：Maka 检出中的 `packages/runtime/src/stream-graph-supervisor-tools.ts`。

<a id="model-experience"></a>
## 模型体验

模型看到三个工具和一个提示词段落（本包其余部分对模型不可见）：

- `view_agent_graph` — 一个图的限界快照：工作状态、截断的记录摘要、就绪意图、`omitted` 计数，以及用于分页活动状态的不透明 `nextCursor`。
- `update_agent_graph` — 每次调用一个持久化决策：添加工作（每项经 `targetKind` 选一个身份字段，`replaces` 指向既有工作 id）、停止目标，或用已提交结果 id 完成图。`idempotencyKey` 使重试安全。
- `yield_agent_graph` — 在图工作继续时协作式结束主管回合；主机会在下一次持久化检查点唤醒根会话。
- `orchestration:graph` 提示词段落 — 指示主管让出而非轮询、按波次报告结果、绝不臆造工作 id。

<a id="known-limitations-and-deferred-work"></a>
## 已知限制与后续工作

- 无图注册表：存储没有创建/列举图的操作，因此对未知图执行 `view_agent_graph` 返回空快照而非 `unknown_graph`（该错误码为后续注册表切片预留）。
- 已完成工作保持 `requested`（P2 状态模型没有终止性工作状态），因此 `yield_agent_graph.pendingWorkCount` 统计 requested 调度行——活动（声明/意图）由唤醒门反映，而非计数。
- 工具为每个 addWork 项接受显式 `workId`（相对 Maka 的扩展），使一次更新可确定性地引用自己的新工作；确定性派生 id 仍是默认。
- 本插件通过手搭测试组合演练；经 Loader 启动 cordis.yml 的组合测试（packages/AGENTS.md 产品插件策略）推迟到集成切片。可选组合补丁 [`apps/cli/config/examples/graph/cordis.yml`](../../../apps/cli/config/examples/graph/cordis.yml) 展示了预期的挂载方式：主机行提供控制器，本包挂载为图根会话的预设行。
