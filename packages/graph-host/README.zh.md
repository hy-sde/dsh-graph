---
description: "Agent Graph 主机侧装配：把 P1–P5 切片接到真实 harness 服务（存储、子代理、worktree、压缩、空闲），并提供主管工具使用的 agentGraphController 服务。"
kind: "package-reference"
---

# @hy-sde-org/dsh-graph-host

[English](README.md) | 中文

## 摘要

`dsh-graph-host` 是 Agent Graph 的主机侧装配（Maka 移植，P6–P7a 切片）：打开图控制单元（P1 `dsh-graph-control`），在真实 harness 接缝之上构建算子执行器（P3 `dsh-graph-executor`），通过运行身份账本喂给不带身份的 P3 记录汇，构造主管工具运行其上的 `AgentGraphController`（P4 `dsh-tool-graph`），并在图根会话的空闲边界驱动唤醒投递（P5 `dsh-graph-wakes`）。本包不提供任何工具或提示词段——模型可见面留在 `dsh-tool-graph`；本包提供服务与持久化 `graph/change` 事件流。

`graph-host` Cordis 插件声明 `inject: ['agents', 'sessions', 'subagents', 'git', 'compaction']`，并在自身 fiber 上发布两个服务：

| 服务 | 导出常量 | 值 |
| --- | --- | --- |
| 主管工具用 `ctx.get` 解析的控制器 | `SERVICE_AGENT_GRAPH_CONTROLLER` | `dsh-tool-graph` 的 `AGENT_GRAPH_CONTROLLER_SERVICE` = `'agentGraphController'` |
| 整个装配句柄 | `SERVICE_GRAPH_HOST` | `'graphHostServices'` |

插件 `Config` 字段：`rootSessionId`（必填——图根；只有此会话能驱动主管工具，它也拥有 `graph/change` 事件）、`subagentProvider`（必填——转发给 `subagents.start` 的提供者名；随附的进程内提供者为 `spawn`）、`backend?`（承载图控制单元的存储后端名，默认 `'sqlite'`）、`worktreeRepoRoot?` / `worktreeBaseBranch?` / `worktreeMaxSlots?`（worktree 池几何参数）、`maxNewActivations?`（每次协调驱动的算子新激活上限，默认 `4`）。

## 目录

- [使用本包](#use-this-package)
- [理解实现](#understand-the-implementation)
- [进一步探索](#further-exploration)
- [模型体验](#model-experience)
- [已知限制与后续工作](#known-limitations-and-deferred-work)

<a id="use-this-package"></a>
## 使用本包

主机行位于**主机组合**——它注入主机服务并发布新服务，因此没有任何可按会话键控的内容。工具行位于图根会话的**代理预设**；它只消费已发布的控制器。

```yaml
# host composition (loaded before any session)
- id: graph-host
  name: '@hy-sde-org/dsh-graph-host'
  config:
    rootSessionId: session-01HABC   # the graph root session id
    subagentProvider: spawn         # one-shot in-process subagent provider
    backend: sqlite                 # backend owning the graph control unit
    maxNewActivations: 4            # new operator activations per drive (default 4)
```

```yaml
# agent preset composition for the graph root session
- id: tool-graph
  name: '@hy-sde-org/dsh-tool-graph'
```

插件在根代理发布（`agent/created`）时装配，或在其已存活时立即装配，并随其 fiber 撤回服务。装配也可显式构建：

```ts
import { createGraphHostServices } from '@hy-sde-org/dsh-graph-host'

const services = await createGraphHostServices({
  rootSessionId: session.id,
  storage: { open: descriptor => backend.kv.open(descriptor) },
  subagents: { provider: 'spawn', start: (name, request) => ctx.subagents.start(name, request) },
  worktrees: {
    repoRoot,
    acquire: options => worktreeEngine.acquire(options),
    list: () => worktreeEngine.list(),
  },
  compaction: { request: sessionId => agent.runMaintenance(signal => engine.compactNow(agent, signal).then(() => {})) },
  sessionEvents: { appendGraphChange: (sessionId, data) => session.append('graph/change', data).then(() => true) },
  idle: { observe: (rootSessionId, onIdle) => agent.ctx.on('agent/status', ({ status }) => {
    if (status === 'idle') onIdle(rootSessionId)
  }) },
  resolveParentAgent: rootSessionId => ctx.agents.get(SessionId(rootSessionId)),
})

await services.attachGraph('graph_g1') // registers the controller, starts the wake runtime
const snapshot = await services.snapshotFor('graph_g1') // bounded P6 session projection
await services.emitGraphChange(session.id, 'graph_g1', snapshot, snapshot.revision)
await services.dispose() // stops wake delivery, cancels children, closes the store
```

### 接线

仓库随附可选组合补丁 [`apps/cli/config/examples/graph/cordis.yml`](../../../apps/cli/config/examples/graph/cordis.yml)。从开发检出应用：

```sh
dsh web --patch apps/cli/config/examples/graph/cordis.yml
```

补丁携带上面给出的 `graph-host` 行，`rootSessionId` 保留为 `<ROOT_SESSION_ID>` 占位符，`subagentProvider: spawn`；请把占位符替换为部署的图根会话 id。预设行（`@hy-sde-org/dsh-tool-graph`、`@hy-sde-org/dsh-graph-projection`）挂载在图根会话的代理预设中，如上面的 `tool-graph` 所示。

<a id="understand-the-implementation"></a>
## 理解实现

### 平面拆分：主机行对预设行

`graph-host` 是**主机平面行**：注入 `agents`、`sessions`、`subagents`、`git`、`compaction`，并发布 `agentGraphController` 与 `graphHostServices`。注入在任何会话存在之前解析，因此没有可按其键控的代理——从预设域发布会把服务藏进主机和每个其他会话都看不到的地方。`dsh-tool-graph` 是**代理平面行**：它用 `ctx.get` 机会式消费控制器，缺失时在加载期响亮失败。预设行绝不发布服务；此处的唯一预设行贡献是工具注册。

### 真实服务之上的门面

装配需要的每个服务都被收窄为 `src/types.ts` 中的结构性门面，使装配器及其测试永不拓宽到完整服务类。插件映射每个门面：存储后端（`ctx.get(storageBackendServiceKey(name))`）、子代理（`ctx.subagents.start`）、git worktree 引擎（`primaryRepoRoot` + `acquireWorktree`/`listWorktrees`）、压缩（经代理维护接缝的 `ctx.compaction`）、会话事件（`session.append('graph/change', …)`）、空闲（转交唤醒运行时的 `agent/status === 'idle'` 观察）。

### 恰好一次激活守卫

P3 执行器按算子序列化激活但不记忆声明，因此装配器用 `OncePerClaimGraphExecutor` 包裹：一个声明 id 对应一次子运行（每个装配），已完成（或进行中）激活后的重新驱动返回折叠的记录而不启动第二个子代理。守卫是进程本地的——持久化声明行仍是重启权威。

### 记录折叠与无持久化记录的决定

P3 `recordSink` 收到的 `AgentGraphRecordSourceEvent` **不带**算子/会话身份（事件只带 `runId`），因此 `GraphRunIdentityLedger` 从执行器自己执行过的子启动中恢复身份（执行器把 `claim.targetRunId` 同时复制进启动输入与发出的事件），`InProcessGraphRecordSource` 按算子×会话键折叠终态事件。`readCommittedAgentGraphProjection`（P2）对该来源的回放与对子会话日志的回放完全相同，因此记录推导在一个主机进程内保持 P2 一致。

记录是派生状态，本切片**刻意保持进程内存**：子会话上的持久化 `graph/record` 事件在 `KNOWN_SESSION_EVENT_TYPES` 重新生成前会被持久化读路径拒绝（P6 范围），而把终态事件归因到子会话日志需要改 P3。持久化权威是一组控制行——调度、声明、供应、唤醒。**主机重启会丢失算子记录**，直到后续切片加入带归因的持久化事件；届时会话投影回退到调度/声明状态（如 `claimed`），直到该切片落地。

### 会话投影与 graph/change 契约

`buildSessionGraphProjection` 把控制器整图快照变成有界的 P6 载荷。每条 `graph/change` 事件携带 `{ graphId, snapshot, revision }`：

```ts
interface SessionGraphProjection {
  readonly schemaVersion: 1
  readonly graphId: string
  readonly status: 'active' | 'closed'
  readonly revision: number
  readonly closed: boolean
  readonly work: readonly {
    readonly workId: string
    readonly status: 'requested' | 'claimed' | 'executing' | 'stopped' | 'finished' | 'failed'
    readonly instruction: string // truncated to 300 characters with a trailing ellipsis
    readonly operatorId?: string
    readonly inputCount: number
  }[]
  readonly omitted: { readonly work: number; readonly records: number; readonly inputs: number }
  readonly pendingWake: boolean
  readonly updatedAt: number
}
```

工作项有上限（`SESSION_PROJECTION_MAX_WORK = 128`：请求头部加终态尾部），记录尾随到 `SESSION_PROJECTION_MAX_RECORDS = 64`，指令到 `SESSION_PROJECTION_INSTRUCTION_MAX_CHARS = 300`；`omitted` 携带被排除的计数。`pendingWake` 在图的唤醒行为 `pending` 或 `retryable_failed` 时为真。状态优先级：调度停止先赢；然后是算子的终态记录（执行器只在子代理落定后折叠它——`[operator failed] …`/`[operator cancelled]` 摘要映射为 `failed`/`stopped`，其余映射为 `finished`）；然后是声明准入状态（仅进行中）；再是 `requested`。终态记录压过声明准入，因为准入状态没有终态。

`emitChange`（每次调度提交、每次协调、每次唤醒投递后调用）是尽力而为的，按内容指纹去重，并按图串行化，使并发触发对每次指纹变化至多发出一个事件。事件只追加到根会话日志——无界面放置——由 P6 投影层折叠。

### 唤醒投递

`attachGraph` 注册控制器并把唤醒运行时限定到根会话启动。投递只在空闲边界运行（`observeIdle` → `handleIdle`）：投递钩子重新驱动协调器并发出新的 `graph/change`。关闭的图短路为 `superseded`（运行时也自行折叠调度日志：`finish` 更新或图/根目标停止会在不投递的情况下取代唤醒）。失败的投递返回 `retryable_failed`；提供者确认的上下文溢出（`GraphHostContextOverflowError` 或 `overflow === true` 标记）触发运行时的一次压缩恢复，同一唤醒的第二次溢出携带部分快照（`partialResult: true`），使唤醒在尝试上限（`DEFAULT_MAX_DELIVERY_ATTEMPTS = 3`）处耗尽，而不是第三次完整重试。协调后投影读取属于投递的一部分：那里的溢出标记失败触发同一条一次压缩路径。

<a id="further-exploration"></a>
## 进一步探索

- [dsh-graph-control](../graph-control/README.zh.md)（P1）— 本装配打开的持久化存储（调度日志、声明、供应、唤醒）。
- [dsh-graph-stream](../graph-stream/README.zh.md)（P2）— 执行器与控制器驱动的协调器与记录投影折叠。
- [dsh-graph-executor](../graph-executor/README.zh.md)（P3）— 算子执行器及其子运行器/worktree 池接缝。
- [dsh-tool-graph](../tool-graph/README.zh.md)（P4）— `agentGraphController` 上的主管工具。
- [dsh-graph-wakes](../graph-wakes/README.zh.md)（P5）— 本装配接线的空闲门控唤醒运行时。
- [dsh-graph-projection](../graph-projection/README.zh.md)（P6）— 把 `graph/change` 事件折叠进会话图投影。
- `tests/graph-host.spec.ts` — 真实 SQLite 存储 + 假子代理/worktree/压缩/会话事件/空闲。

<a id="model-experience"></a>
## 模型体验

此处不产生任何模型可见的提示词文本。模型看到的是 `dsh-tool-graph` 的三个主管工具；本包的贡献是它们运行其上的控制器服务与追加到根会话日志的 `graph/change` 事件（P6 投影的持久化输入，而非界面元素）。无 KV 缓存或 token 影响：执行器自身不调用模型提供者，而是通过注入接缝启动子代理。

<a id="known-limitations-and-deferred-work"></a>
## 已知限制与后续工作

- **算子记录是进程本地的。** P3 记录汇不带算子/会话身份，且持久化子会话事件在 `KNOWN_SESSION_EVENT_TYPES` 重新生成前会被拒绝，因此 `InProcessGraphRecordSource` + `GraphRunIdentityLedger` 只在内存折叠终态记录：主机重启会丢失它们，会话投影回退到调度/声明状态，直到后续切片加入带归因的持久化事件。
- **恰好一次激活守卫是进程本地的。** `OncePerClaimGraphExecutor` 在内存按声明 id 记忆；重启后重新驱动的声明可能启动第二次子运行，因为持久化声明行记录准入但不记录终态结果。
- **`graph/change` 去重是每个装配且内存内的**：指纹映射在重启时重置，因此重启的主机可能对未变化的图重新发出一个事件。
- **每次挂载一个根会话**：插件 Config 命名单个 `rootSessionId`（唤醒运行时与发射目标以它为范围），且 `agentGraphController` 服务名是每个主机 fiber 的单例——部署多个图根时，每个根需要独立 realm 中的独立主机行，而不是共享一行。
- **终态工作项在持久化调度日志中保持 `requested`**，直到停止/完成更新提交；会话投影从折叠的终态记录推导 `finished`，因此丢失记录的主机重启会再次显示 `claimed`/`executing`（见记录折叠限制）。
