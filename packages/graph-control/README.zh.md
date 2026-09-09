---
description: "Agent Graph 的持久化决策存储：调度更新、恰好一次的意图声明、操作员预置与主管唤醒。"
kind: "package-reference"
---

# @hy-sde-org/dsh-graph-control

[English](README.md) | 中文

## 摘要

`dsh-graph-control` 是 Agent Graph 的持久化决策存储（Maka 移植，P1 切片）。它只持有图真正需要的有状态行——调度更新日志、恰好一次的意图声明、操作员预置和主管唤醒——其余一律不存：记录、路由、就绪意图、工作状态和客户端快照留给后续切片中的派生层（session-projection 折叠）。

本包围绕 Maka 的一条硬规则设计：**持久化声明（含预分配的 turn/run 身份）必须在运行时被要求执行之前写入**，且每个声明/预置转换都以其观察到的调度修订号为前置条件。因此重试只会复用同一个激活身份，而不会第二次调用提供方。所有 ID 均为确定性 sha256（`graph_update_…`、`graph_claim_…`、`graph_operator_…`、`graph_wake_…`），重放天然幂等。

持久化：一个 `KvUnit`（`name: agent_graph`）承载五张权威表；派生唯一性索引在打开时从权威行重建，撕裂写入可自愈而非损坏。存储契约禁止同一单元的并发写入者，因此本存储将全部变更串在一条写链上，每条记录写入均持久化。

本包不提供任何工具、提示词或插件行——由协调器（P2）、执行器适配器（P3）和主管工具（P4）消费。

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

const backend = await ctx[storageBackendServiceKey('sqlite')]
const unit = await backend.kv.open(GraphControlStore.descriptor)
const store = await GraphControlStore.open(unit)

const { update, created } = await store.commitScheduleUpdate(request)
const { claim } = await store.claimIntentAtScheduleRevision(claimRequest, update.revision)
```

每个进程只打开该单元一次：存储层拒绝重复打开，且本存储是该单元上的唯一写链。

<a id="understand-the-implementation"></a>
## 理解实现

- **调度日志**（`schedule`）：只追加决策，修订号 = max+1，按 `updateId` 与源三元组 `(session, run, toolCall)` 幂等；`finish` 不能与 `add_work` 合并；一旦提交 finish，图即关闭。
- **意图声明**（`claims`）：键为 `graphId:intentId`，激活身份唯一性（`(targetSessionId, targetTurnId)` 与 `(targetSessionId, targetRunId)`）由派生索引约束；`claimed → executing → cancelled` 转换以修订号为条件；关闭后拒绝新声明，但既有声明仍可派发。
- **操作员预置**（`provisions`）：确定性 `provisionId`/`operatorId` 使重试采用同一操作员；与声明一样受修订号约束并在关闭后拦截。
- **主管唤醒**（`wakes` + `wake_attempts`）：声明一次后开始尝试（已投递/已替代则拒绝）；以 `waiting_permission | delivered | superseded | retryable_failed` 完成；按根会话（可选图过滤）替代；`recoverSupervisorWakes()` 刻意为空操作——中断的尝试是否真正完成属于运行时事实，协调器（P5）检查运行事实后完成之。本存储从不猜测。

<a id="further-exploration"></a>
## 进一步探索

- [`port_maka.md`](../../../../workspace/port_maka.md) — 移植设计说明与阶段清单。
- Maka 参考：Maka 检出中的 `docs/architecture/agent-graph-stream-scheduling-draft.md`（第 7 章）。

<a id="model-experience"></a>
## 模型体验

无模型可见面。本包是主机侧机制；模型看到的是 P4 切片的主管工具。

<a id="known-limitations-and-deferred-work"></a>
## 已知限制与后续工作

- 无 epoch 表：按设计决策，一个 DSH 会话拥有一个图（每根多图推迟）。
- 多行 CAS 为进程原子（单写链）而非事务原子；崩溃后序列中撕裂的写入在打开时自愈，因为索引是派生的。撕裂写入的声明由协调器检查运行事实恢复，与 Maka 一致。
- 派生工作状态（`requested/stopped/superseded`）、记录、路由、就绪与客户端快照属于后续切片，不在此存储。
