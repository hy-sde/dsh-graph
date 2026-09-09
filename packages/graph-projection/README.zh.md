---
description: "从宿主的 graph/change 发布提供会话当前 Agent Graph 快照；面向组合或调试 graph 投影单元的客户端与维护者。"
kind: "package-reference"
---

# @hy-sde-org/dsh-graph-projection

[English](README.md) | 中文

## 概述

`dsh-graph-projection` 以 `graph` 投影单元提供会话当前的 Agent Graph 快照——完整且有界的 `SessionGraphProjection`（图身份、closed/active 状态、版本、有界工作列表、省略计数、待投递 wake）。宿主（P7）拥有图状态，并在每次图状态变化时以 `graph/change` 会话事件发布一份完整的变更后快照；本单元把这类发布折叠进会话投影接缝（注册表快照、变更流、每条投影载体），与存储无耦合。在已挂载投影注册表的组合中选择它，例如以图导航栏为参考消费者的 Web 应用包；没有注册表的装配不受影响，其消费者读不到 `graph` 键。装配与发布语义在前；折叠内部细节放在下方可折叠的开发者章节中。

## 目录

- [使用本包](#use-this-package)
- [理解实现](#understand-the-implementation)
- [进一步探索](#further-exploration)
- [模型体验](#model-experience)
- [已知限制与延期工作](#known-limitations-and-deferred-work)
- [开发备注](#dev-note)

-----

<a id="use-this-package"></a>
## 使用本包

当客户端需要从完整发布值渲染会话的 Agent Graph 而不耦合图控制存储时，在会话存储与投影注册表旁挂载此插件。只有存在注册表时单元才会注册。

### 组合

```yaml
- name: '@deepseek-ai/dsh-session'
- name: '@deepseek-ai/dsh-session-projection'
- name: '@hy-sde-org/dsh-graph-projection'
```

### 各字段含义

| 字段 | 含义 |
|---|---|
| `schemaVersion` | 线格式版本；本投影为 `1`。 |
| `graphId` | 快照所属的图。 |
| `status` | 图运行中为 `active`，finish 后为 `closed`。 |
| `revision` | 宿主分配的发布版本，每个图严格递增；折叠以它拦截过期重复发布。 |
| `closed` | 终态镜像，供按终态分支的消费者使用。 |
| `work` | 有界工作列表：`workId`、执行期 `status`（`requested`/`claimed`/`executing`/`stopped`/`finished`/`failed`）、有界 `instruction`（≤300 字符）、可选 `operatorId`、`inputCount`。 |
| `omitted` | 宿主裁掉的导航栏预算溢出：work、records、inputs 计数。 |
| `pendingWake` | 是否有一条监督者 wake 待投递到根会话。 |
| `updatedAt` | 宿主发布时刻。 |

wire 值是完整的变更后快照（整值规则）：消费者整体替换，从不合并。宿主每次图状态变化追加一条 `graph/change` 事件：`session.append('graph/change', graphSnapshotToEvent(graphId, snapshot, revision))` ——此处导出的辅助函数构造精确载荷，保证发布版本与快照版本一致。首次发布前单元提供 `null`：会话尚无图，消费者按“尚未可用”而非“空图”处理。

<a id="graph-chip"></a>
### Graph 标签

Web 聊天 UI 通过投影标准席位 `useProjection('graph')` 读取该值；类型化键来自对 `@hy-sde-org/dsh-graph-projection/types` 的仅类型导入——客户端开发期依赖，运行时被擦除（无客户端到 graph 的运行时边）。当投影存在且导航栏有工作项（`work.length > 0`）或图仍为 `active` 时，对话记录渲染该标签；键缺失、首次发布前的 `null`、或已关闭且导航栏为空的图不渲染任何内容。该行显示「由 Agent Graph 继续」，摘要为 `N 个工作项 · active|closed`，样式与压缩标签一致。该标签仅是视图层状态；会话快照从不携带投影值。

### 失败与恢复

没有投影注册表时单元是惰性的：`inject` 使 fiber 保持挂起，不注册任何内容，因此其他装配缺少 `graph` 键。卸载插件会移除该键，因为注册是挂载 fiber 上的 effect。持久缓存行在恢复时经受 schema 校验——包括取值为正的版本与 ≤300 字符的指令界限——损坏的行被丢弃而不会喂坏折叠。

-----

<a id="understand-the-implementation"></a>
## 理解实现

<details>
<summary>实现细节——点击展开</summary>

本节解释快照背后的折叠；可观察行为已在[使用本包](#use-this-package)中完整说明。

### 设计理念

该单元是对已提交 `graph/change` 事件的纯折叠。一个 DSH 会话拥有一个图，因此折叠只对已承载的图替换状态：命名不同 `graphId` 的发布保留既有快照（引用不变，注册表身份门据此让变更流保持安静），仅以水位记录被观察事件——会话的图在生命周期内不切换。版本未推进到已折叠值之上的发布是过期重复发布，返回同一状态引用，重复投递因此零推送。wire 视图就是事件载荷中的快照引用本身（恒等），这正是纯内部变化保持安静的来源：无克隆、无重渲染。

### 源码地图

| 文件 | 职责 |
|---|---|
| [`src/index.ts`](src/index.ts) | 插件入口：`inject`、在挂载 fiber 上注册单元 |
| [`src/projection.ts`](src/projection.ts) | 折叠：状态 schema、版本门、wire 视图、发布辅助函数、指令界限 |
| [`src/types.ts`](src/types.ts) | `graph` 投影键声明、`graph/change` 事件增广与 wire 类型的唯一归属 |
| — | 不发布运行时不变式伴生入口：本包仅拥有一个纯投影折叠，`session-projection` 会对其对外值执行 schema 校验；用同一实现重新折叠同一日志只会复制实现，无法比较独立维护的观测；宿主据以发布的持久行由 graph-control 负责。 |

### 折叠规则

- 不相关事件返回同一状态引用；注册表的两道 `Object.is` 门由此把变更流压到真正的快照变化。
- 首次发布采纳任意图 id（尚无承载图）；此后不同 id 的发布只记入水位。
- 过期（未推进）版本返回同一状态；匹配图的发布原子地替换快照、水位与版本。
- 对外值是载荷快照的引用——不复制、不截断、不重推导；界限是宿主的发布约定，在此处仅于持久状态边界校验。

</details>

-----

<a id="further-exploration"></a>
## 进一步探索

当单元约定不够用时阅读以下页面。它们从驱动单元的注册表逐步进入宿主据以发布的图包。

- [会话投影子系统](../../../docs/subsystems/session-projection.zh.md)——驱动单元并提供快照与变更流值的注册表。
- [会话投影注册表包](../../session/session-projection/README.zh.md)——单元注册所依据的注册表约定。
- [图流包](../graph-stream/README.zh.md)——派生态流层，其计划与记录供宿主构建发布快照。
- [工具图包](../tool-graph/README.zh.md)——有界模型可见快照词汇，客户端投影以更紧的导航栏预算复用它。

-----

<a id="model-experience"></a>
## 模型体验

无，因为 `graph` 单元把已发布的宿主快照折叠成面向客户端的读模型，不注册任何面向模型的内容。

#### KV Cache 影响

无；本包从不组装或发送提供方请求。

## 已知限制与延期工作

<a id="known-limitations-and-deferred-work"></a>


这些限制说明投影提供什么、单元何时缺失。它们是当前包约束。

- **wire 值在每次推送中携带完整快照**——纯内部变化被身份门忽略，但真实重发布仍投递整个工作列表（整值规则）；把工作拆成按需读取推迟到数百个工作条目的会话真正需要时。
- **每会话一个图**——不同 `graphId` 的发布不会替换既有快照，会话无法同时承载两个图；多图会话需要按图分键。
- **状态与界限信任宿主**——投影校验持久形状与 ≤300 字符指令界限，但实时发布路径信任宿主在追加前分配执行期状态并截断指令。
- **首次发布前为 `null`**——挂载插件即注册键，但值在宿主发布前保持 `null`；消费者必须处理“尚未可用”状态。
- **仅在组合了投影注册表时挂载**——其他装配不提供 `graph` 键，其消费者把不存在读取为无图能力。

<a id="dev-note"></a>
### 开发备注

<details>
<summary>维护者的工作上下文——点击展开</summary>

无。

</details>
