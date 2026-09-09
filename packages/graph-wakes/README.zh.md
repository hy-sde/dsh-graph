---
description: "Agent Graph 的主机侧唤醒投递：在 P1 控制存储之上按空闲门控投递持久化主管唤醒。"
kind: "package-reference"
---

# @hy-sde-org/dsh-graph-wakes

[English](README.md) | 中文

## 摘要

`dsh-graph-wakes` 是 Agent Graph 主管唤醒路径的投递半侧（Maka 移植，P5 切片）。`dsh-graph-control`（P1）已拥有持久化唤醒行（`pending | running | waiting_permission | delivered | superseded | retryable_failed`）；本包提供进程内运行时，在**下一个空闲边界**把到期唤醒送入其所属根会话——绝不在 turn 运行中——并通过存储自身的 begin/complete CAS 持久化结算每次尝试。

运行时刻意解耦。`GraphWakeRuntime` 接收存储接缝（`GraphControlStore` 结构性满足）、接收 `{ graphId, wakeId, rootSessionId, snapshotVersion }` 的 `deliver` 钩子（P6 接线重新驱动 `AgentGraphCoordinator` 并投递主管检查点——运行时绝不导入协调器）、可选的 `onCompact(sessionId)` 压缩钩子，以及可注入的空闲观察器。投递在构造上即空闲门控：唯一投递入口是 `handleIdle()`，注入的观察器把 `agent/status === 'idle'` 边界转发给它——与 Schedule 包使用的状态观察接缝相同——因此运行时从不自行启动投递。

本包不提供任何工具、提示词或插件行——P6 的主机接线负责挂载运行时并提供观察器与投递钩子。

## 目录

- [使用本包](#use-this-package)
- [理解实现](#understand-the-implementation)
- [进一步探索](#further-exploration)
- [模型体验](#model-experience)
- [已知限制与后续工作](#known-limitations-and-deferred-work)

<a id="use-this-package"></a>
## 使用本包

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

<a id="understand-the-implementation"></a>
## 理解实现

### 空闲门控投递

投递只从 `handleIdle(sessionId?)` 进行。`start(rootSessionId?)` 注册注入的 `observeIdle` 观察器，生产接线用 `agent.ctx.on('agent/status', …)` 且只响应 `status === 'idle'`，与 Schedule 插件完全一致；观察器仅转发到 `handleIdle`。空闲信号在 turn 之间触发，因此唤醒绝不会在 turn 运行中被投递；交付钩子本身必须经由所属 agent 的维护/空闲接缝运行唤醒（P6），与 `ScheduleRuntime` 在 `followup()` 前用 `runMaintenance` 认领空闲相位的做法相同。`start` 从不同步投递。观察器只上报存活的根，因此枚举自然限定在真正能接收 turn 的会话；重新武装计时器只是重新驱动的提示，仍经由 `handleIdle` 进入。

### 尝试与持久化状态机

对每个到期唤醒，运行时用确定性尝试 id（`graphWakeAttemptId(wakeId, attemptIndex)`，`turnId` 同值）调用存储的 `beginSupervisorWakeAttempt`：存储在其写链下原子递增 `attemptCount` 并把唤醒从 `pending → running`，一旦行为已投递或已被取代即拒绝。随后通过 `completeSupervisorWakeAttempt` 结算投递钩子结果：

| 投递结果 | 持久化尝试状态 | 重新武装？ |
| --- | --- | --- |
| `delivered` | `delivered` | 否 |
| `waiting_permission` | `waiting_permission` | 否（停驻至主机恢复） |
| `superseded` / `stopped` | `superseded` | 否 |
| `retryable_failed` | `retryable_failed` | 是（除非已耗尽） |

### 重试、退避与终态失败

`retryable_failed` 结果会在 `outcome.nextAttemptAt` 或 `now + 30 秒 × 尝试序号` 重新武装该唤醒（默认值：`DEFAULT_RETRY_BACKOFF_MS`、`DEFAULT_MAX_DELIVERY_ATTEMPTS = 3`）；重新武装是进程本地的，分段计时器会为所属根重新驱动 `handleIdle`。一旦 `attemptCount` 达到 `maxAttempts`，该唤醒对本运行时即为终态——存储没有 `failed` 状态，因此行停留在上限处的 `retryable_failed` 且不再重新武装。

### 上下文溢出恢复

当投递返回带 `overflow: true` 的 `retryable_failed` 时，运行时对每个唤醒至多调用一次 `onCompact(rootSessionId)`，然后立即重新武装，允许一次有界的部分结果投递（投递钩子决定部分结果形态并以 `partialResult: true` 上报）。若部分尝试本身溢出，或溢出在压缩后到达但未声明为部分结果，运行时拒绝第三次相同的完整投递：唤醒进入终态（`exhausted` 恢复状态）。未接线 `onCompact` 时，溢出唤醒在首次尝试后终止。P1 尝试行没有 `partialResult` 列，因此这些标记保持进程本地（见限制）。

### 停止抑制

当图的调度日志表明图已停止或关闭时，到期唤醒**不投递**而被取代：任何带 `finish` 的更新（图已关闭——协调器 `isClosed` 的权威），或 `targetId` 为根会话或图 id 的日志停止。后者即图级停止约定：本切片一个会话一张图，因此图停止以面向根身份的停止记录，而工作项停止（work id）绝不取消唤醒。运行时镜像 Schedule 包"停止取消到期记录"的行为：每次扫掠折叠持久化日志，而非信任进程状态。

### 单飞与幂等

重叠的空闲信号合并为一次串行扫掠（单飞；在结算期间到达的信号会重新请求），存储 CAS 使一个尝试行对应一次投递：第二个运行时或重试扫掠以同一尝试 id 开始时观察到 `acquired: false` 而不投递。存储故障经 `onError` 上报并把尝试行留在 `running` 供主机恢复；抛异常的投递钩子以 `retryable_failed` 结算并携带钩子消息。

### 重启持久性

运行时需要的全部状态都在存储中：同一存储上的新 `GraphWakeRuntime` 看到相同的唤醒行，在下一个空闲处重新武装遗留的 `retryable_failed` 唤醒（如 Maka 的 `recover`），且只要尝试计数仍允许就不会拒绝。退避时间戳与溢出标记是进程本地的，刻意不持久化。

<a id="further-exploration"></a>
## 进一步探索

- `packages/graph/graph-control`（P1）：本运行时结算所基于的持久化唤醒行与 begin/complete CAS。
- `packages/graph/graph-stream`（P2）：`AgentGraphCoordinator`，由 P6 投递钩子重新驱动。
- `packages/schedule/schedule`——本运行时镜像的观察模式（`agent/status === 'idle'` + `agent.whenIdle`）。
- Maka 参照：Maka 检出中的 `packages/runtime/src/agent-graph-supervisor-wake.ts`（权威唤醒语义）。
- `tests/graph-wakes.spec.ts`：真实 sqlite 存储 + 假空闲观察器与投递钩子。

<a id="model-experience"></a>
## 模型体验

无模型可见面。本包为主机侧机制；模型看到的是 P4 切片的主管工具，P6 接线把 `deliver` 变成模型可见的检查点 turn。

<a id="known-limitations-and-deferred-work"></a>
## 已知限制与后续工作

- P1 尝试行没有 `partialResult`（或溢出）列：一次压缩/一次部分的标记是进程本地的。重启会清除它们，因此在 `attemptCount` 上限停止重试前还可能发生一次完整尝试；超过上限仍不可能。
- 终态失败没有存储状态：耗尽的唤醒停留在尝试上限处的 `retryable_failed`。本切片中 P1 的状态并集已固定。
- 此处不恢复 `running` 唤醒（begin 与 complete 之间崩溃）：被中断的尝试是否真的完成是运行时事实，P1 的 `recoverSupervisorWakes` 空操作把该事实留给主机接线（P6）。
- `waiting_permission` 唤醒被停驻且绝不重试；权限响应恢复（Maka `notifyPermissionResponse`）推迟到 P6。
- 跨进程协调不在范围内：与协调器一样，运行时是进程本地的，因此持有同一存储的另一进程不会唤醒本运行时。
- 图级停止约定（`targetId` 等于根/图 id 的日志停止）在此定义；工作项停止绝不抑制唤醒。P4 必须以根身份提交图停止，抑制才会生效。
