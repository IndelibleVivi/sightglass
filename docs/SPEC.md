# Sightglass

**产品与工程规格（v0.4）**\
**仓库名：`sightglass`**\
**产品名：Sightglass**\
**状态：已接受的产品规格；本轮已接受扩展见 [Retrieval extension](RETRIEVAL-SPEC.md)，2026-10-04 选择性驻留／离线瘦身扩展见同一文档；实现与限制见 [Current state](current-state.md)**

公开示例中的人物、会话和称呼均为合成占位。该规格表达产品目标，不授予任何真实账号访问权限；实际访问必须由账号 owner 显式授权。

v0.4 preflight 修正：冻结 WGO baseline、稳定/可变身份证据分层、显式时间语义、coverage-aware on-demand hydrate、observation-based updates、exact pending-delivery replay、资源稳定绑定，以及真实 canary 前 daemon gate。

2026-10-05 MCP 界面修订：保留十三项能力，采用默认 brief／显式 diagnostic、保守新请求页量、软 JSON 预算与明确续读动作，详见 [MCP contract](MCP-CONTRACT.md)。Diagnostic 只改变回包详略，仍要求 strict source／policy 验证；updates 的 exact replay 与 ACK 不受 profile 裁剪。

---

## 0. 一句话定义

这是一个运行在 用户自己 Mac 上的、local-first、read-only 的私人微信阅读产品。

它让明确授权的 reader 可以在不要求用户 反复截图、复制、下载、上传和补充人物背景的前提下，自行完成：

- 找到正确的微信会话；
- 读取逐条原始消息；
- 向前、向后翻上下文；
- 从上次阅读位置继续查看新消息；
- 搜索历史原文；
- 在群聊中聚焦某个成员，读取她在指定时间、关键词或消息范围内的原始发言，并按需展开每条发言的上下文；
- 阅读图片、PDF、文本文件、链接卡片、转发记录等附件与资源；
- 明确知道读取是否完整、是否存在缺口、附件是否缺失、结果是否被截断。

产品不以“总结微信群聊”为中心。WGO 是可复用的成熟基础设施与可选知识增强层，不能成为本产品的运行前提、监控范围边界或数据模型中心。

---

## 1. 产品承诺

当用户说：

> 看看示例甲刚才说了什么。

授权 reader 应当能够：

1. 找到与“示例甲”有关的正确会话；
2. 读取最近消息；
3. 发现其中有一张图片和一个 PDF；
4. 查看图片预览、读取 PDF 相关页面；
5. 往前翻消息，理解她在回应什么；
6. 告诉用户读取范围、资源范围与任何缺口。

用户也可以说：

> 只看示例甲今天在这个群里说了什么。

授权 reader 应当先在该群中解析出稳定的成员身份，再返回示例甲本人发送的文字、图片、文件、回复等消息；需要现场时，可同时展开每条命中消息前后的邻近消息。若群里有同名成员，必须返回候选并要求消歧，不能按昵称猜一个。

过一会用户说：

> 她又回了。

授权 reader 应当从自己的上次阅读位置继续读取，且：

- 不修改微信已读状态；
- 不推进 WGO monitor checkpoint；
- 不改变任何 summary bookmark；
- 不向聊天对方留下操作痕迹；
- 不要求用户再次搬运现场。

---

## 2. 核心目标

### 2.1 P0 目标

1. **完整现场阅读**：群聊与私聊同等支持，逐条消息、顺序、发送者、时间、消息类型和资源引用保持可追溯。
2. **上下文导航**：recent、context、updates、range、single-message 五种阅读动作具备稳定 contract。
3. **成员聚焦阅读**：群聊成员是一级查询维度；支持按稳定 participant identity 读取某个人的全部发言、限定时间与关键词、选择 only / with-context 视图，并保留附件与回复关系。
4. **附件阅读**：图片、PDF、文本/代码文件、链接卡片和转发聊天记录进入首个可用版本。
5. **来源可信度**：source incomplete、generation changed、资源缺失、结果截断必须显式返回，不能伪装成“没有消息”。
6. **独立产品线**：独立 repo、独立 reader state、独立权限、独立 read model；不依赖 WGO monitor selection。
7. **WGO 能力复用**：复用经过验证的微信数据库读取、解密、snapshot、source inventory、stable message identity、图片解码与附件归档经验。
8. **纯读取**：MCP 不发送、不撤回、不标已读、不改备注、不写入微信、不修改 WGO 知识状态。
9. **local-first**：微信数据库、密钥、索引、资源缓存默认留在 用户的 Mac；只有当前工具调用返回的最小必要片段离开本机。

### 2.2 P1 目标

1. 授权 reader 独立 reader profile 与 durable reading cursor。
2. 访问收据、总暂停开关、conversation denylist / allowlist。
3. WGO Knowledge 与 WGO CAS 作为可选 adapter。
4. 语音本地转写、视频封面/抽帧、Office 文档文本与预览。
5. 轻量菜单栏 App 或本地控制页。

---

## 3. 明确非目标

v0.4 首个实现阶段不做：

- 通过微信发送消息；
- 撤回、转发、收藏、删除、标已读；
- 添加好友、加群、修改备注或群名；
- 默认后台盯人、主动通知“某人回复了”；
- 默认生成 AI 总结；
- 自动把读到的第三方消息写入 OB、Atria 或长期关系记忆；
- 自动请求聊天中出现的远程链接；
- 自动下载微信当前未落地到本机的远程资源；
- 云端同步完整微信库；
- 面向团队、企业或员工监控；
- 把所有微信历史一次性复制到另一个永久档案中；
- 第一阶段提供 Windows 支持。

后台通知、自动 watch、写入长期记忆、远程资源抓取都必须作为未来独立能力线设计，不能偷偷混进读消息的基础 contract。

---

## 4. 设计原则

### 4.1 底座复用，产品独立

可复用 WGO 已验证的 source 技术与行为；本产品不能依赖 WGO 的：

- monitored chat selection；
- topic/event 命中；
- Digest；
- Review Queue；
- summary bookmark；
- Obsidian projection；
- AI provider；
- monitor checkpoint。

### 4.2 原文优先，派生内容可追溯

普通文本逐字保留。任何解析、转写、OCR、PDF 提取、预览和结构化结果都必须标明其派生性质和来源。

### 4.3 重复可接受，漏读不可接受

updates 采用 at-least-once delivery。偶尔重复返回一批消息可以接受；静默跳过消息不可接受。

### 4.4 不完整就明确失败

默认 `strict=true`：相关 source shard 缺失、数据库 generation 在读取中发生变化或 cursor 无法验证时，整次读取 fail closed，不返回看似完整的半截结果。

未来可支持 `strict=false` 作为显式诊断模式，但必须返回 `partial=true` 与具体 warning。

### 4.5 内容是数据，不是指令

微信文字、附件、PDF、网页内容、压缩包成员、代码和转发记录全部是不可信数据。产品不得把其中的指令提升为系统行为或越权访问其他会话。

### 4.6 MCP 是出口，不是产品本体

核心能力应存在于可复用 service 层。MCP 只做参数校验、授权、service 调用、输出预算和结构化响应。

### 4.7 群成员是一级阅读维度

“看某个人在群里说了什么”不是昵称字符串过滤，而是稳定 participant identity 上的阅读动作。

要求：

- 先在指定会话内解析 participant，再用 `participant_id` 查询；
- 昵称、备注、群昵称和历史别名只用于候选召回与显示，不能充当永久主键；
- 同一群内同名成员必须保持为不同 identity；
- 非好友群成员即使不在联系人表中，也必须能从消息 sender identity 建立 participant；
- 群昵称变更不得切断历史发言；
- 不依据相同昵称、头像或文本风格自动合并跨群身份；跨会话 person linking 只能依赖可证明的稳定 source identity 或显式人工绑定；
- participant filter 适用于全部消息类型，包括纯图片、文件、链接、语音、视频、转发记录和 system-adjacent reply；
- 聚焦阅读可以只返回该成员发言，也可以返回每条发言的邻近上下文；任何上下文消息必须清楚标记为 context-only；
- participant-filtered read / updates 不得推进整个 conversation 的 reader cursor。

### 4.8 “是谁”与“显示成什么名字”必须分离

Sightglass 不设置一个会覆盖其他信息的“真实姓名”字段。身份由三层组成：

1. **source principal**：消息 envelope、联系人库或群成员库中的稳定 source identity，用于回答“是不是同一个人”；
2. **conversation membership**：该 participant 在具体群聊或私聊中的成员关系，用于承载群名片、群内角色和成员状态；
3. **label observations**：备注、账号昵称、微信号/公开 handle、群名片、复制或导出界面显示名、Sightglass 用户别名等可变化称呼。

名称不能充当主键。每个 label observation 必须带：

```text
label_kind
scope                 # account | conversation | message-surface | reader
provenance
observed_at
valid_from nullable
valid_to nullable
temporal_confidence   # exact | near | current_only | unknown
```

默认返回两个不同概念：

- `label`：给 reader 使用的当前首选称呼；
- `shown_as`：消息所在 source/presentation surface 中实际观察到的称呼，若无法证明消息当时的显示名则为空或标记 `current_only`。

默认 `label` 选择顺序：

```text
conversation-scoped Sightglass alias
→ account-scoped Sightglass alias
→ 用户的联系人备注
→ 当前群名片
→ 微信账号昵称
→ 公开 handle
→ opaque fallback
```

`shown_as` 不参与身份合并。复制记录显示账号昵称、群里显示群名片、联系人列表显示备注，都可以同时为真。

### 4.9 时间语义必须显式

Sightglass 不得依赖 daemon、MCP bridge 或测试进程的隐式系统时区。

持久化时至少区分：

```text
source_time_raw          # source 原始时间值与单位
sent_at_utc              # 规范化 UTC instant
observed_at_utc          # Sightglass 实际观察时间
reader_timezone          # reader profile 显式 IANA timezone
```

规则：

- 排序和 cursor 使用规范化 instant + source tie-break，不使用格式化时间字符串；
- “今天 / 昨天 / 本周”等范围由 reader profile 的显式时区解析，首个配置可以读取 Mac 当前时区，但必须写入配置；
- API 优先接收绝对时间边界；自然语言相对时间由调用方解析后传入；
- 转发聊天记录中的时间文本若无法可靠解析，保留为 `sent_at_text`，不能冒充 canonical timestamp；
- DST、跨时区旅行和系统时区改变不得静默改变历史查询边界。

### 4.10 人工别名与身份纠错属于本地控制面

Sightglass alias、participant merge/split、source-key rebind 都是本地 operator 写操作，不通过 read-only MCP 暴露。

所有纠错必须：

- 进入 append-only correction ledger；
- 更新当前 identity projection；
- 不改写 immutable message observations；
- 可审计、可回滚；
- 对已有 anchor、cursor、pending delivery 采用明确的继续、重放或失效规则。

---

## 5. 推荐总体架构

```text
┌─────────────────────────────────────────────────────────────┐
│                       User’s Mac                            │
│                                                             │
│  WeChat local data                                          │
│        │                                                    │
│        ▼                                                    │
│  WeChatSourceProvider                                       │
│  - discover/decrypt/snapshot                                │
│  - source inventory                                         │
│  - canonical message reads                                  │
│  - source FTS candidates                                    │
│  - resource resolution                                      │
│        │                                                    │
│        ▼                                                    │
│  sightglassd  (long-running local daemon)                    │
│  - window.db                                                 │
│  - ReaderService                                             │
│  - ResourceService                                           │
│  - Reader profiles / policy                                  │
│  - delivery journal / audit receipts                         │
│  - local Unix socket                                         │
│        │                         ▲                           │
│        │                         │ optional                  │
│        │                  WGO adapters                       │
│        │                  - knowledge                       │
│        │                  - attachment CAS                  │
│        ▼                                                    │
│  sightglass-mcp  (thin bridge)                               │
│  - stdio initially                                           │
│  - local HTTP / secure outbound tunnel later                 │
└───────────────┬─────────────────────────────────────────────┘
                │ minimum requested content only
                ▼
           MCP clients / authorized readers
```

### 5.1 进程边界

#### `sightglassd`

长驻本地 daemon。它持有微信受保护数据访问权限、维护 source 状态和 `window.db`，并通过 Unix domain socket 提供只读本地 API。

#### `sightglass-mcp`

薄 MCP bridge。它不直接读取微信数据库，不保管解密 key，不自行扫描源文件；只连接 `sightglassd`。

#### `sightglassctl`

本地 operator CLI，用于：

- status / doctor；
- 初始化；
- 配置 reader；
- pause / resume；
- deny / allow conversation；
- cache inspection / cleanup；
- synthetic test / fixture verification。

#### 可选控制面

后续加入菜单栏 App 或本地 Web UI。MCP 本身不暴露权限修改工具。

### 5.2 初始实现允许的临时简化

为了尽快验证 contract，M1–M3 的纯 synthetic 阶段可以先让 MCP 进程与 daemon 同进程运行，但必须保留 service 边界与 provider interface。

任何真实微信数据 canary 之前必须完成 daemon + thin bridge 边界；按本 spec 的里程碑顺序，这一 gate 位于 M4。短命 MCP 进程不得直接承担真实受保护微信目录的长期访问。

---

## 6. Repo 结构

建议新建独立私有 repo：

```text
sightglass/
├── pyproject.toml
├── README.md
├── AGENTS.md
├── NOTICE.md
├── docs/
│   ├── SPEC.md
│   ├── ARCHITECTURE.md
│   ├── SECURITY.md
│   ├── SOURCE-ADAPTER.md
│   └── MCP-CONTRACT.md
├── src/sightglass/
│   ├── contracts/
│   │   ├── common.py
│   │   ├── messages.py
│   │   ├── resources.py
│   │   ├── receipts.py
│   │   └── errors.py
│   ├── source/
│   │   ├── base.py
│   │   ├── direct_wechat.py
│   │   ├── parser.py
│   │   ├── identity.py
│   │   ├── snapshot.py
│   │   └── resource_resolver.py
│   ├── model/
│   │   ├── db.py
│   │   ├── schema.py
│   │   ├── migrations.py
│   │   └── repositories.py
│   ├── reader/
│   │   ├── service.py
│   │   ├── cursors.py
│   │   ├── deliveries.py
│   │   ├── search.py
│   │   └── aliases.py
│   ├── resources/
│   │   ├── service.py
│   │   ├── cache.py
│   │   ├── images.py
│   │   ├── pdf.py
│   │   ├── text.py
│   │   └── safety.py
│   ├── policy/
│   │   ├── readers.py
│   │   ├── capabilities.py
│   │   └── audit.py
│   ├── ipc/
│   │   ├── server.py
│   │   ├── client.py
│   │   └── protocol.py
│   ├── mcp/
│   │   ├── server.py
│   │   ├── tools.py
│   │   └── projection.py
│   ├── adapters/
│   │   └── wgo/
│   │       ├── knowledge.py
│   │       └── resources.py
│   ├── daemon.py
│   └── cli.py
└── tests/
    ├── fixtures/
    ├── unit/
    ├── integration/
    ├── contract/
    └── security/
```

首版可以适当合并文件，但不能把 SQL、微信 XML 解析、授权和 MCP tool implementation 混在一个巨型文件中。

---

## 7. WGO 复用与独立边界

### 7.1 应复用的能力

Codex 必须先审查 WGO 当前 `main`，但开工时先记录并冻结 exact commit SHA、license/NOTICE 与被审查文件清单；后续报告不能只写“当前 main”。确认实际文件与 contract 后，优先复用或移植以下行为：

- 微信数据库定位与解密；
- WAL / snapshot 一致性；
- source inventory 与 degraded state；
- 多 message shard 发现与合并；
- stable source message identity；
- 联系人与会话解析；
- 微信 FTS 候选搜索；
- XML 消息解析；
- V2 图片解码；
- 本地资源定位；
- content-addressed attachment archive 的经验与测试；
- macOS 长驻 process identity 与受保护目录访问经验。

预期审查起点包括但不限于：

```text
core/wechat_db.py
core/decryptor.py
core/source_inventory.py
core/image_decoder.py
core/attachment_archive.py
core/key_extractor.py
现有 monitor cursor / source traversal 代码
现有 MCP read tools
```

这些路径只是起点，不是允许盲目复制的固定 API。

### 7.2 禁止形成的耦合

核心产品运行时不得 import 或依赖：

```text
WGO monitor selection
TopicMonitor
WGO AI provider
Daily Digest
Obsidian exporter
Review Queue
summary bookmark
knowledge event acceptance
```

### 7.3 推荐复用策略

第一阶段采用“选择性移植 + 行为测试对齐”：

1. 从 WGO 中识别 source-neutral 代码；
2. 保留完整 provenance、版权与许可证头；
3. 移植到本 repo 的 `source/`；
4. 用 synthetic fixtures 对齐行为；
5. 避免新产品运行时依赖 WGO app；
6. 后续观察两边 drift，再决定是否抽成真正共享 package。

不要在第一阶段同时重构 WGO 主仓库与新产品，避免跨仓大爆炸。

### 7.4 许可证与 provenance

WGO 当前受 AGPL 系许可证与上游 NOTICE 约束。任何代码复制、派生或分发都必须保留 provenance，并在未来对外分发前单独做许可证审查。

私人本机阶段也必须保留 NOTICE、来源文件说明与对应 commit/reference，不能等公开发布时再回头追溯。

### 7.5 可选 WGO adapter

核心稳定后再加入：

```python
class WGOKnowledgeAdapter:
    search_knowledge(...)
    get_event(...)
    get_topic(...)
    read_digest(...)

class WGOResourceAdapter:
    find_archived_object(resource_identity)
    open_archived_variant(...)
```

WGO adapter 不得扩大 reader 的 conversation 权限；knowledge hit 必须能回到 `message_id` / source reference，无法回链时明确标记 `source_linkage_missing=true`。

---

## 8. Source Provider Contract

核心 interface：

```python
class WeChatSourceProvider(Protocol):
    def health(self) -> SourceHealth: ...
    def list_accounts(self) -> list[SourceAccount]: ...
    def list_conversations(self, account_id: str) -> list[SourceConversation]: ...
    def resolve_conversation(self, account_id: str, query: str) -> list[ConversationCandidate]: ...

    def list_participants(
        self,
        account_id: str,
        conversation_source_id: str,
        snapshot: SourceSnapshot,
    ) -> list[SourceParticipant]: ...

    def resolve_participant(
        self,
        account_id: str,
        conversation_source_id: str,
        query: str,
        snapshot: SourceSnapshot,
    ) -> list[ParticipantCandidate]: ...

    def read_recent(
        self,
        account_id: str,
        conversation_source_id: str,
        limit: int,
        snapshot: SourceSnapshot,
    ) -> SourceMessagePage: ...

    def read_range(
        self,
        account_id: str,
        conversation_source_id: str,
        *,
        after: SourceSortKey | None,
        before: SourceSortKey | None,
        direction: Literal["forward", "backward"],
        limit: int,
        snapshot: SourceSnapshot,
        participant_source_ids: tuple[str, ...] = (),
    ) -> SourceMessagePage: ...

    def search_candidates(
        self,
        account_id: str,
        query: SearchQuery,
        snapshot: SourceSnapshot,
    ) -> list[SourceSearchCandidate]: ...

    def get_message(
        self,
        account_id: str,
        source_message_id: str,
        snapshot: SourceSnapshot,
    ) -> SourceMessage | None: ...

    def resolve_resource(
        self,
        account_id: str,
        source_message_id: str,
        resource_index: int,
        snapshot: SourceSnapshot,
    ) -> SourceResourceResolution: ...

    @contextmanager
    def snapshot(self, account_id: str) -> Iterator[SourceSnapshot]: ...
```

### 8.1 Participant source identity

Provider 应返回结构化 source identity evidence，不能只返回一个显示名。

解析优先级：

1. 消息 envelope 中明确且经版本验证的 sender principal ID；
2. 群成员数据库或联系人数据库中的稳定 internal ID；
3. 可验证地指向同一 principal 的其他 stable source key；
4. conversation-local sender token；
5. 最后才是 alias-only unresolved actor。

Source identity evidence 必须分层：

```text
principal keys
  internal_username       # provider 内部稳定账号 ID
  openim_or_variant_id    # 仅在已验证稳定时可作为 principal key

conversation-local keys
  conversation_sender_id  # 只在一个会话内稳定，必须带 conversation scope

mutable discovery evidence
  public_handle           # 用户可见微信号/handle，可缺失、变化或被重新使用
  account_nickname
  contact_remark
  group_card
  message_surface_label
```

`public_handle` 属于 mutable evidence / label，不得进入 canonical principal-key 唯一映射，也不能因值相同自动链接两个人。Provider 必须同时返回：

```text
source_identity_keys
membership evidence
label observations
resolution_state
identity_confidence
```

Provider 不能仅凭显示名合并成员。无法稳定解析时返回 `resolution_state="conversation_local"`、`"alias_only"` 或 `"unresolved"`；false merge 比暂时保留两个 participant 更严重。

每个账号必须建立一个明确的 self participant。群聊中，同 shard `real_sender_id → Name2Id.user_name` 与 raw `mapped_sender + ":\n"` prefix 精确一致时构成 stable member evidence，并优先于 status；两者不一致或 envelope 不可验证时必须 unresolved，不能误归 self。没有 member envelope 的本机群消息仍可通过 outgoing/status/account evidence 解析到 self；私聊 incoming/outgoing 不能只靠显示名区分。当前受支持的 macOS source build 将 direct incoming 状态 `0|1` 与 outgoing 状态 `2|3` 分开解析；其它状态不猜测 peer/self，而是保留为 unresolved sender。System/recall conversation event 不绑定 human sender、强制 `is_self=false`，因此不能进入 participant speaker filter。

转发聊天记录、截图 OCR、手动复制文本中的 sender label 默认是 nested/unresolved identity。除非存在可验证的 canonical message linkage，不能把嵌套 sender 名称绑定到外层 participant。

手动复制或导入的聊天文本如果只含显示名而没有 source sender ID，只能形成 `message_surface` label observation。只有在时间、会话、正文和 canonical source message 足以重合时，才允许 reconciliation 到稳定 participant。

账号 identity 不能由数据库目录路径决定。Source path 只是 installation locator；优先使用稳定 self principal / account metadata 建立 `source_account_key`。若无法取得稳定账号 key，必须标记低置信度，路径迁移后不得自动把两个 namespace 合并。

### 8.2 SourceHealth

至少包含：

```json
{
  "configured": true,
  "available": true,
  "account_count": 1,
  "source_state": "complete",
  "fresh_as_of": "ISO-8601",
  "inventory_digest": "opaque",
  "shard_counts": {
    "present": 4,
    "missing": 0,
    "key_missing": 0,
    "cache_only": 0,
    "unreadable": 0
  },
  "warnings": []
}
```

### 8.3 SourceSnapshot

一次多步骤读取必须绑定同一 snapshot / generation set。读取期间 source identity 发生变化时返回 `SOURCE_GENERATION_CHANGED`，不得拼接不同 generation 的消息。

### 8.4 搜索候选不可直接作为最终证据

微信 FTS 或其他索引只负责召回候选。最终返回前必须回到 canonical source 或 `window.db` 中经过 canonical observation 的消息进行验证。

### 8.5 Coverage 与按需 hydrate

`window.db` 默认不是完整微信镜像。首版采用按需 hydrate：

- conversation/session metadata 可以增量索引；
- 交付所需消息、必要上下文与 participant identity evidence 按本地驻留策略进入 read model；按需扫描的全部候选不因此永久入库；
- 不因为某个 participant 尚未被观察到就断言其不存在；
- `find_conversations`、`find_participants`、search 与“not found”响应必须带 coverage，例如 roster 是否完整、观察到的消息时间范围、是否仅覆盖活跃会话；
- 后续若提供 full backfill，必须作为独立显式操作与 receipt，不得偷偷发生。

### 8.6 Source traversal 与 reader updates 分离

Source traversal cursor、timeline pagination cursor、reader update cursor 是三个不同概念：

- source traversal cursor：确保每个 shard 被可靠观察和去重；
- timeline cursor：按消息时间线前后翻页；
- reader update cursor：按 Sightglass 的单调 `observation_seq` 交付“新观察到的变化”。

连续 sync frontier 与独立 observed windows 分开：已有 1–100 后，foreground recent 观察 181–200，不能把 frontier 改成 200；后台仍从 100 后逐批恢复 101–180。Historical backfill 从 contiguous floor 前继续，不能被一个更早的独立岛误导到全局 earliest 前。History 与 forward complete 分别须有 exhaustion proof，内部 gap／legacy unverified 仍存在时不得 claim complete；已有完整 proof 且 recent 上界不越过 frontier 时保留 complete。Physical source 未改变也不能跳过待重验证或未完成的 forward recovery。

若 provider 无法证明 source traversal key 严格单调，必须使用 bounded overlap + stable message dedupe，并安排 reconciliation；不能只按 `sent_at` 前进。在有效 retained update scope 内，晚到、补录或恢复出来的旧时间消息必须能以 `late_arrival=true` 进入 updates，而不能被 timeline cursor 静默跳过；§8.7 的有限缓存若已不足以维持该 scope，明确 expire/rebaseline。

### 8.7 选择性驻留与按需阅读（2026-10-04 accepted extension）

ReaderPolicy 的访问授权和本地 residency 是两个独立维度。`keep`（长期）、`recent`（近期）、`on_demand`（按需）仅作用于已获授权的聊天；排除访问属于 ReaderPolicy，不能用 residency setting 扩大 allowlist。新配置和新发现的授权聊天默认 `on_demand`。升级保留既有库存的保护状态，不自动把历史正文变成可淘汰缓存。

- `keep` 在设置之后持续观察；旧历史 backfill 是显式、有范围的独立操作。长期模式仍受 owned-storage 预算，不能承诺无限空间。
- `recent` 默认 30 天，期限可配置，同时受每聊天与全局字节预算约束。裁剪后报告实际 resident coverage；旧历史仍可按需回源。
- `on_demand` 不持续收录正文；读取、搜索和上下文只 admission 本次交付所需的小范围缓存，受 TTL、每聊天与全局字节预算约束。冷查询可 preparing、分段扫描、取消；本地 FTS 未覆盖 source 全部历史。
- 本地 operator 可以列表、按占用估计排序和批量设置；修改未来收录与清理旧副本是分别预览、分别执行的动作。共享索引／页归因标明 estimate，不把逻辑 payload 减少当作 filesystem 已回收。

历史 observation/traversal progress 与当前 resident coverage 分开。主动过期的区间不成为后台自动补洞目标；本地 context、索引和 status 不能把曾观察过的区间冒充仍可本地读取。所有 source admission 入口共享驻留决定，包括 sync、queued backfill、search preparation、retrieval、context、refresh、资源与 voice 依赖，不能仅在后台过滤。

按需重读旧内容不是新 updates。驻留 epoch 改变或被回收证据已不足以维持旧 update scope 时，明确 expire/rebaseline；不静默 ACK，也不承诺在有限缓存中维持无限历史变化检测。有效 pending spool 继续 exact replay；阅读 lease、active resource/voice job 只保护实际依赖的 rows/objects，不能永久 pin 整个聊天。指向已释放内容的 cursor 或 opaque binding 明确 stale/expired，不能拼出新 payload 代替旧视图。

保留 actor identity、alias/correction 和 reader progress 等不可仅凭 source 重建的状态。已获批准释放的正文如果后来也从微信 source 消失，可能无法找回；source unavailable、未扫描、preparing 与 limited coverage 均不能当作没有匹配。Shared CAS 继续以全部 FK liveness 决定回收；residency 不授权 semantic egress、remote index 删除或任何网络抓取。

---

## 9. 本地 Read Model：`window.db`

微信是 source content authority。`window.db` 保存标准化消息 projection，以及稳定 ID、alias/correction 历史、阅读状态、source observations、资源绑定、搜索与审计；其中 reader state 和本地历史不能仅凭 source 无损重建。

SQLite 设置：

- WAL mode；
- foreign keys on；
- busy timeout；
- 单 writer；
- schema version；source、candidate 与 installed version 分别以 [Current state](current-state.md) 为准；
- 既有小型 migration 在 DDL 前要求 exact verified recovery；SG-059 的大型 schema/backend conversion 则必须显式 stopped-only candidate，不挂在普通 startup，任一步失败保持原 DB/runtime pair；
- 文件权限 `0600`；
- parent directory `0700`。

### 9.1 主要表

#### `accounts`

```text
account_id PK
source_namespace UNIQUE        # installation/source namespace，不等于账号 identity
source_account_key nullable    # 稳定 self principal/account evidence
identity_confidence
reader_timezone                # explicit IANA timezone
current_display_name
first_seen_at
last_seen_at
active

UNIQUE(source_account_key) WHERE source_account_key IS NOT NULL
```

#### `conversations`

```text
conversation_id PK
account_id FK
source_conversation_id
kind                # group | direct | official | unknown
current_title
first_seen_at
last_seen_at
last_message_at
visibility_state    # active | hidden | unavailable
UNIQUE(account_id, source_conversation_id)
```

#### `conversation_aliases`

```text
conversation_id FK
alias
alias_kind          # title | remark | member-derived | user-defined
valid_from
valid_to nullable
source
```

#### `participants`

```text
participant_id PK
account_id FK
current_reader_label nullable
is_self
actor_kind          # person | official | system | unknown
resolution_state    # stable | conversation_local | alias_only | unresolved
identity_confidence # exact | strong | weak | unknown
first_seen_at
last_seen_at
```

#### `participant_source_keys`

一个 participant 可以拥有多个 source key；canonical linking 依赖这里的稳定证据，不依赖名字。

```text
source_key_id PK
participant_id FK
account_id FK
key_kind             # internal_username | stable_variant_id | conversation_sender_id
key_value
scope_conversation_id nullable
stability            # stable | conversation_local
principal_eligible   # bool
provenance
first_observed_at
last_observed_at
active
```

`public_handle` 不放入此表的 canonical unique mapping；它进入 label / mutable identity evidence。

SQLite 中 `NULL` 不参与普通 UNIQUE 冲突判断，因此必须使用两个显式 partial unique index：

```sql
UNIQUE(account_id, key_kind, key_value)
  WHERE scope_conversation_id IS NULL;

UNIQUE(account_id, key_kind, key_value, scope_conversation_id)
  WHERE scope_conversation_id IS NOT NULL;
```

原始 source key 只保存在本机，不默认通过 MCP 返回。外部 contract 使用 opaque `participant_id`。

#### `participant_labels`

账号级或 reader 级称呼：

```text
label_id PK
participant_id FK
label
normalized_label
label_kind           # sightglass_alias | contact_remark | account_nickname | public_handle | message_surface
scope_kind           # account | reader | message_surface
reader_id nullable
observed_message_id nullable  # message_surface 必须能回到具体消息证据
provenance
observed_at
valid_from nullable
valid_to nullable
temporal_confidence  # exact | near | current_only | unknown
active
```

#### `identity_corrections`

append-only operator ledger：

```text
correction_id PK
action               # alias_set | alias_unset | merge | split | source_key_rebind | rollback
subject_json
reason nullable
created_at
operator_identity
supersedes_correction_id nullable
```

纠错只改变当前 projection / binding，不改写历史 message observations。

#### `conversation_members`

```text
membership_id PK
conversation_id FK
participant_id FK
source_membership_id nullable
current_group_alias nullable
resolution_state      # stable | conversation_local | alias_only | unresolved | ambiguous
first_seen_at
last_seen_at
last_message_at nullable
UNIQUE(conversation_id, participant_id)
```

#### `conversation_member_labels`

群名片和其他会话级显示名属于 membership，不属于 participant 本体：

```text
member_label_id PK
membership_id FK
label
normalized_label
label_kind           # group_card | roster_display | message_surface | sightglass_alias
observed_message_id nullable  # exact message-surface evidence
provenance
observed_at
valid_from nullable
valid_to nullable
temporal_confidence  # exact | near | current_only | unknown
active
```

成员索引必须能从实际出现过的消息构建，不要求该成员先存在于联系人表。群成员列表不完整时，已观察到的 sender 仍应可被解析和检索。

当前 roster 中的群名片不能被静默回填成历史消息“当时显示名”。只有存在当时或接近当时的 observation，才能标记为 `exact` 或 `near`；否则仅为 `current_only`。

#### `messages`

当前投影，不是唯一历史证据：

```text
message_id PK
account_id FK
conversation_id FK
source_message_id
sent_at
sort_primary
sort_tie
sender_id nullable
sender_membership_id nullable
sender_label_snapshot_json
kind
text nullable
structured_json
first_seen_at
last_seen_at
current_state       # present | recalled | deleted | unavailable | unknown
current_generation_id
projection_epoch
first_observation_seq nullable
current_observation_seq nullable
UNIQUE(account_id, source_message_id)
```

Schema v5 以 `projection_epoch` 证明当前 row 使用现行 provider/parser/identity 语义投影，并以 first/current observation sequence 把该 row 固定到一个 materialized watermark。旧 schema row 迁移后这三列保持 `NULL`，必须经正常 source admission 重新观察后才可进入 local read plane；迁移不伪造 observation、不批量重写历史、也不删除旧 row。

Schema v9 增加 `source_conversation_state.coverage_version`、`contiguous_floor_position`、`history_complete` 与 `forward_complete`，以及 `source_read_windows` 的 typed lower/upper source sort-key 区间。`tail_*` 表示有连续证据的 sync frontier；已观察消息的 min/max 仍是 observation bounds，不能证明中间连续。正常 validated recent/range/context 页记录 contiguous window，source sync/backfill 用已验证 anchor 连接区间，多个互不相交窗口保留为明确 gap。Migration 只加 metadata，旧 row 默认 unverified，保留全部消息／历史 observation；原 complete flag 没有 proof 时对 reader 降级。后台以 bounded forward pages 从最旧位置逐批重验证 stock coverage，checkpoint 随成功 admission 持久化，重启后继续，不在 startup 全量扫描或重写历史。

#### `message_observations`

immutable append-only：

```text
observation_seq INTEGER PRIMARY KEY AUTOINCREMENT
observation_id UNIQUE
message_id FK
observed_at
source_generation_id
state
payload_digest
parsed_json
parser_version
raw_payload_ref nullable
reason_code nullable
```

`observation_seq` 是本地单调交付轴，用于 updates；它不等于消息 sent time。同一 current episode 可 idempotent 重观察，但 history-wide payload 去重不能吞掉 A→B→A 的最终 transition：最后的 A 必须追加新 observation，并使 current projection 指向实际当前 payload，不能用该消息历史的 MAX sequence 指向 B。

Operator-only observation maintenance 提供只读有界 inspect（每批最多 500 条 projection）与显式 repair；repair checkpoint 在 DB 内原子持久化，重启可续批，失败或 storage floor 拒绝不推进 checkpoint。每个 mismatch 最多检查 100 条 prior observations；有匹配 body/source evidence 时追加 repair episode，无可验证证据时 invalidates local projection、等待正常 source 重观察。它不改写 immutable history、不覆盖合法 sender/member identity correction、不接触 source。Repair revision 与 link/lexical versions 令既有 local cursor 失效；pending delivery 仍用 immutable spool exact replay。

如一条消息后来被撤回，不覆盖旧 observation；新增 `recalled` observation，当前投影更新为 recalled。

**Absence is not deletion**：在 bounded page、缺失 shard、cache-only 状态或普通增量扫描中没看到一条旧消息，不能据此把它标成 deleted/recalled。只有显式 recall/system evidence，或覆盖范围与 generation 都可证明完整的 authoritative reconciliation，才允许产生相应状态 observation。

#### `resources`

```text
resource_id PK
message_id FK
source_resource_key nullable
source_ordinal
kind
mime_type nullable
original_name nullable
declared_size nullable
declared_hash nullable
availability
resolver_json
first_seen_at
last_seen_at
```

`resource_id` 不能只依赖 parser 当前输出顺序。优先使用 source attach ID/hash/稳定 descriptor 形成 `source_resource_key`；没有稳定 key 时，以 message identity + source payload evidence 建立首次绑定，之后 parser 升级不得静默把已有 `resource_id` 指向另一资源。

显式索引：

```sql
UNIQUE(message_id, source_resource_key)
  WHERE source_resource_key IS NOT NULL;

UNIQUE(message_id, source_ordinal)
  WHERE source_resource_key IS NULL;

INDEX(message_id);
```

两个 partial unique 索引都无法服务裸的 `WHERE message_id = ?`：该谓词不蕴含它们各自的 `source_resource_key IS [NOT] NULL` 条件。schema v4 因此为 `resources.message_id` 增加一个完整索引。没有它时，每次按消息查找 resources 都退化为经主键索引的全表扫描；`upsert_message` 对每条入库消息都做一次这种查找，于是 source sync 的入库阶段随 `resources` 行数线性变慢，最终超过 source worker 的 20 秒预算，使 live 读取整体不可用（见 [Current state](current-state.md)）。

#### `resource_objects`

```text
object_digest PK
local_path_internal
mime_type
byte_size
origin              # wgo_cas | wechat_local | generated_preview | private_cache
created_at
last_verified_at
```

#### `resource_bindings`

```text
resource_id FK
object_digest FK
variant             # original | preview | page:N | thumbnail | extracted_text
created_at
UNIQUE(resource_id, variant)
```

#### voice 持久层与领域服务（schema v3，仅内部）

`voice_jobs`、`voice_batches`、`voice_batch_items`、`voice_batch_events` 是 schema v3 新增的内部持久层。`src/sightglass/voice/` 已有直接使用 `WindowDB` 的纯领域 service/repository；输入必须是服务端已验证的 reader/account/binding、message-bound selection 和 recipe。`wechat_read_transcripts` 是这一层唯一的 MCP 出口，daemon 侧另有生命周期内 bounded voice worker。**默认安装仍然没有配置识别器**：worker 以 disabled 存在、从不运行 fake recognizer，工具只读已提交页面并如实报告 `unavailable`；配置齐备时 daemon 装配唯一的生产识别器（见下节），它完全在本机运行，不接 ASR provider、不联网、不写 source，也不表示 installed runtime/live 已升级。

- `voice_jobs`：`(account_id, resource_id, resource_revision, recipe_digest)` 上 partial unique index 保证每份输入同时最多一个 `pending/leased/running/blocked` 活跃 job；`ready/failed/cancelled` 释放槽位。`input_digest` 与 `result_digest` FK 到 `resource_objects`；CHECK 约束覆盖 attempt、fencing、时长与字节上限。领域服务提供 lease/start/complete/fail、过期 lease recovery、transient requeue 与 replacing-context takeover；owner/fencing token 拒绝迟到写入，recovery 只把 leased/running 恢复为 pending，不自动重试 blocked。
- `voice_batches`：`batch_id` 即 opaque reading token；绑定 reader/account/binding、selection/recipe digest 与 `voice_policy`（auto/cached/off）。`selection_digest` 是 canonical JSON（sorted keys、紧凑分隔符、UTF-8）的 SHA-256，内容为 version=1、reader_id、account_id、有序 `[message_id, resource_id, resource_revision]` 列表与 recipe_digest，不包含时长。未过期且 scope/policy 相同的批次复用 token/manifest；TTL 默认 24 小时，在创建时固定、不滑动。off 或无 voice 不创建批次或 job。
- Admission 默认首次最多 3 项/300 秒，后续每个新 cursor step 最多 12 项/300 秒，全局 pending/leased/running 最多 32 项/600 秒；未知时长按单项上限 120 秒预留。step 超额保持 queued（对外交付 not_scheduled），单项超限、全局无预算或 binding 不符则 rejected；rejected 永久不重排。cached policy 仅复用 ready，cache miss 不创建 job；blocked 复用既有 job，不自动重试。
- `voice_batch_items`：`(batch_id, ordinal)` 复合 PK；`job_id` nullable，被拒绝的 item 无 job。`admission_step` 首次为 0，后续取已验证 cursor 的 event_id；只有 event_id 大于批次已记录的最大 admission_step 才推进 queued items，相同 cursor 重放不刷新预算。
- `voice_batch_events`：`event_id` 自增不复用；`(batch_id, item_ordinal)` 复合 FK，`item_ordinal` nullable 表示 batch 级 event；`result_digest` FK 到 `resource_objects`。服务只追加事件，指向私有 content-addressed JSON 状态快照；初始快照另存 duration_ms/recipe_json 供后续 admission 使用，历史事件不从可变 job 重建。同一终态结果重复写入不增加事件。
- `get_transcripts` 每页交付一条不可变事件，按 event_id 递增；cursor 通过 HMAC 绑定 kind/batch/reader/account/binding/event_id，并检查事件归属。篡改、错 scope、未知 token/事件使用 `CURSOR_INVALID`，过期批次使用 `CURSOR_STALE`。无新事件时保留输入 cursor；`has_more_results_now` 仅表示当前还有事件，不代表任务全部完成。
- Coverage 动态包含 selected、ready、pending（合并 leased/running）、not_scheduled、blocked、failed、empty、cancelled 八项。`processing_complete` 要求无 pending 且无 queued；`text_coverage_complete` 要求所有 selected 均有非空 ready 文本。序列化结果标记为 `derived_transcript`，不冒充原消息。
- Daemon 侧 `wechat_read_transcripts` 分三段执行：短 reader gate 内验证 reader/account/binding/pause/会话 policy 并接受下一 step 额度；释放 gate、foreground claim 与 tool slot 后进入独立有界 waiter（默认 8000 ms，服务端上限 15000 ms，允许 0，恒短于 25 秒工具 deadline，系统级最多 2 个）；再次短 gate 用当前 policy 复验并组装出站页。waiter 由提交事件、pause/deny/reload 唤醒，调用方断开只释放自己的 waiter，容量耗尽立即返回 `capacity_exhausted` 正常结果。worker 由 `start`/`stop`/`reload` 与 source worker 对称管理，替换上下文开始时接管遗留 lease：所有 held lease 归还队列并 +1 fencing token。

#### 本地识别器：SILK → PCM → Apple SpeechAnalyzer（四段）

生产识别器是一个 job 一段、顺序固定的四段管线；每一段都只看到上一段的**已验证输出**，任何一段的失败都不产生部分结果。

**A. Capture**（`voice/capture.py`）。在 resource service 自身的两段 snapshot 语义内 `read_resource(resource_id, mode="original", max_bytes=MAX_SILK_BYTES)` 读取该 job 的 exact original——capture 不另开 source 捷径。随后复核：payload 必须是 `content_kind="audio"` 且 mime 恰为 `audio/silk`、非空、不超上限；`resource_context` 的 `resolver_json.active` 必须为真、`binding_fingerprint` 必须存在且等于 job 记录的 `resource_revision`；resource 所属 `account_id` 必须等于 job 的 account；该 account 仍在 `active_accounts()` 且 `account_binding_id` 等于 job 记录值。message identity **不取自 job 行**（`voice_jobs` 只存 resource binding），而是从上述 resource context 读回。通过后写入 `voice-work/` 下 `O_NOFOLLOW|O_CREAT|O_TRUNC`、mode 0600、fsync 的暂存文件（目录 700、拒绝符号链接），并钉住 `sha256` input digest。job 字段不全、resource 缺失/非 active/account 不匹配/revision 未记录或已变 → `RESOURCE_BLOCKED`；pause、`SOURCE_GENERATION_CHANGED`、`SERVICE_TIMEOUT` → 同码 retryable；其余 resource 错误 → `RESOURCE_BLOCKED(reason="resource_<code>")`。

**B. Decode**（`voice/decoder.py` + `voice/_decode_child.py`）。解码永远发生在独立子进程 `python -m sightglass.voice._decode_child`，输入与输出只经继承的 fd 传递（绝不进 argv）。child 侧：输入字节上限、`RLIMIT_FSIZE` 的 PCM 上限、显式 `pcm_bytes > max` 检查、0 字节结果视为 `empty_decode` 失败、`normalize_envelope` 只接受 `#!SILK_V3`（可选微信 `\x02` 前缀）。parent 侧：`run_bounded` 用独立 session 启动、select 有界排空 stdout/stderr（各 64 KiB）、墙钟 timeout 到期即 SIGTERM 宽限后 SIGKILL **整个 process group**。PCM 规格固定为 16 kHz / mono / s16le 并写进 recipe。不可用、信封不支持、损坏、截断、超限 → `RESOURCE_BLOCKED`（其中解码器被杀于 `SIGXFSZ` 或 `EFBIG` → `pcm_too_large`）；墙钟超时 → `SERVICE_TIMEOUT(retryable=True)`。XML 声称的时长**不**放宽任何上限。

**C. Recognize**（`voice/apple.py` + `swift/sightglass-transcribe/SightglassTranscribe.swift`）。每个 job 起一个预编译 helper 子进程（`scripts/compile-voice-helper.sh`，Swift + `SpeechAnalyzer`+`SpeechTranscriber`），命令行固定 `--pcm <私有暂存路径> --locale <显式 BCP-47>`；helper 只消费 final 结果、排除 volatile、不做翻译/润色/补词，stdout 恰好一行 JSON（schema `sightglass.voice-transcript.v1`，含 text、segments、locale、asset_status、model、os_version 与 `volatile_excluded`）。子进程的 stdout 上限 2 MiB、stderr 64 KiB、墙钟超时由配置 `voice.helper_timeout_seconds` 控制（默认 120，1–600），超时同样杀整个 group；环境只放 allowlist 变量并过滤含 KEY/TOKEN/SECRET/PASSWORD/CREDENTIAL 的名字。退出码 2（模型/语言资产不可用）、4（用法）、报告不合 schema 或文本超长 → `RESOURCE_BLOCKED`；退出码 3 与其他失败 → `SERVICE_UNAVAILABLE(retryable=True)`；超时 → `SERVICE_TIMEOUT(retryable=True)`。helper 不打开麦克风、不下载模型。

**D. Commit**（`voice/apple.py` → `voice/service.py`）。结果连同 content-free provenance 一起进既有 content-addressed store，并在同一短事务内走既有 `complete` 路径（owner + fencing token 在服务端复核）。provenance schema 为 `sightglass.voice-provenance.v1`：

| 分组 | 字段 |
| --- | --- |
| `recipe` | `engine`（`sightglass.voice.apple-silk.v1`）、`decoder`（如 `pysilk/0.2.8`）、`decoder_envelope`（`wechat_prefix`\|`plain`）、`pcm{sample_rate,channels,sample_format,version}` |
| `input` | `resource_revision`、`input_digest`、`silk_bytes`、`pcm_bytes`、`pcm_frames`、`decoded_duration_ms`、`declared_duration_ms` |
| `recognizer` | `backend`、`helper`、`helper_version`、`locale`、`asset_status`、`model`、`os_version`、`segments`、`volatile_excluded` |
| `derived` | `kind="derived_transcript"`、`translation=false` |

provenance 只描述推导身份与规格，不含路径、密钥、原始音频或任何 source 标识。暂存 SILK 与 PCM 无论成功失败都在 `finally` 内删除；worker 启动时另做一次非递归清扫，删除超过 1 小时的遗留暂存文件。调度语义仍是：`RESOURCE_BLOCKED` 永久不重试，retryable 错误 0.5 秒起指数退避、最多 3 次尝试。

#### `reader_profiles`

```text
reader_id PK
display_name
auth_token_hash
policy_json
active
created_at
updated_at
```

#### `reader_timeline_cursors`

用于 recent/range/speaker pagination；分页位置与 delivered reader progress 分开。当 speaker query 的至多 10,001 个 candidate 全不匹配，或 system omit 隐藏整页时，signed continuation 以最后实际 scanned candidate 为边界，即使 messages 为空也不能丢失 next cursor。Cursor schema v2 绑定 query/sender/time/system-policy scope；scan-only boundary 不算该页已交付、不能推进 timeline/update cursor 或 ACK。

```text
reader_id FK
conversation_id FK
scope_kind           # conversation | participant | filter_set
scope_key            # "*" | participant_id | stable filter digest
committed_sort_primary
committed_sort_tie
committed_message_id nullable
updated_at
UNIQUE(reader_id, conversation_id, scope_kind, scope_key)
```

读取某个成员的发言时使用 participant-scoped timeline cursor；它不能推进 `scope_kind="conversation"` 的全会话 cursor。

#### `source_scan_cursors`

Sightglass 内部 source traversal 状态，不属于任何 reader：

```text
account_id FK
conversation_id FK
source_shard_key
source_generation_id
cursor_token
overlap_policy_json
updated_at
UNIQUE(account_id, conversation_id, source_shard_key)
```

#### `reader_update_cursors`

updates 只按本地单调 observation sequence 提交：

```text
reader_id FK
conversation_id FK
scope_kind
scope_key
committed_observation_seq
updated_at
UNIQUE(reader_id, conversation_id, scope_kind, scope_key)
```

#### `reader_deliveries`

```text
delivery_id PK
reader_id FK
conversation_id FK
scope_kind           # conversation | participant | filter_set
scope_key
from_observation_seq
to_observation_seq
projection_schema_version
payload_digest
payload_ref           # private 0600 spool / exact serialized response
status                # pending | acknowledged | expired
created_at
acknowledged_at nullable
expires_at nullable
```

`payload_digest` 单独不足以保证 replay。Pending delivery 必须保存足以重放**同一批、同一顺序、同一 projection**的私有 payload 或 immutable observation-item manifest。每个 `(reader, conversation, scope_kind, scope_key)` 同时最多一个 pending delivery，并通过 transaction / unique partial index 保证并发安全。

Ack 幂等；alias、parser 或 source generation 在 pending 期间变化时，未 ack 调用仍重放原 delivery，不临时重算成另一批。

#### `access_receipts`

禁止保存消息正文：

```text
receipt_id PK
reader_id
tool_name
conversation_id nullable
scope_kind nullable      # conversation | participant | filter_set
scope_digest nullable    # 不含昵称/正文的稳定摘要
message_count
resource_count
bytes_returned
started_at
completed_at
outcome
warning_codes_json
```

### 9.2 数据保留

默认：

- message observations 按 §8.7 的 residency 生命周期保留；既有历史库存默认受保护，只有明确 scope preview/apply 才可释放；
- 原始附件不因“出现过”就自动长期归档；
- 预览采用受限 cache；
- raw XML/source payload 可配置为不保存、仅存 digest，必要时回源读取；
- 用户可手动清理 resource cache，不破坏 message metadata；清理以所有共享 FK liveness（binding/derivation/job input+result/event result）为准，先 commit row 再 unlink，孤儿文件 24h 宽限、不递归；
- pending delivery 所依赖的 payload / observations 在 ack、明确过期并可从 committed cursor 安全重建之前不得清理；
- `window.db` 会保存已观察消息的正文 projection；真实 canary 前 `doctor` 必须报告承载 volume 的磁盘加密状态，未启用时给出高可见 warning，由用户明确接受或启用加密。

选择性驻留扩展取代“所有已观察正文无限保留”的未来 admission 默认；它不自动删除既有库存。保留范围内的 observation 仍是不可变 state episodes：同值同 provenance 的 active label 重观察只刷新 freshness，真实 A→B→A 与 message-surface evidence 不能被编码优化合并。新 observation 使用 versioned lossless BLOB，digest 和 identity 基于原始 bytes，legacy TEXT 继续可读；离线转换必须直接编码原始 UTF-8 bytes，并验证 decode 等价，不能 JSON reserialize。`window.db` 混合可按授权 scope 释放的正文和不可凭 source 重建的 ACK/correction/binding state，不能整库视为 disposable cache。

FTS 只作为 casefolded literal AND 的必要条件加速器；canonical current-source 验证仍是结果权威。目标 contentless-delete trigram index 不保留另一份全文，保留 `detail=none`，需要实际 SQLite ≥3.43 capability probe。不得与 `columnsize=0` 组合；不支持的 query 继续有界可续 fallback。Recipe/generation 与 schema 的转换明确 fence 旧 cursor，不能把半建 index 标 ready。

旧 observation 编码、lexical backend 替换和物理 compact 合并为 stopped-only、可续的候选构造，普通 daemon startup 不执行大型转换。冻结输入与 exact verified recovery point 必须来自同一 committed boundary；保留 `(rowid, stable identity)` 映射、AUTOINCREMENT `sqlite_sequence` 高水位、correction history、reader state、有效 binding 与 immutable pending spool。候选完全验证后才可切换 runtime/DB pair；跨卷候选先复制到 active filesystem 的 staging、验证并 fsync，再同卷 rename。计入 backup/candidate/temp/WAL 与 physical free floor；不能先删旧 active DB 换 headroom，不能增大 daemon limits 或漏报临时文件绕过预算。真实清理范围、候选写入、migration 与 activation 需 operator 的独立精确授权。

Daemon 对 owned DB/WAL/spool/cache/staging/backup 做统一 admission accounting：soft 4 GiB、hard 6 GiB、filesystem floor 2 GiB，额外 maintenance reserve 256 MiB，均可由停机 operator 配置。Soft 暂停历史 backfill 与新增 voice work；hard/free-floor 拒绝新增 admission，返回 retryable `STORAGE_PRESSURE`，且不推进 failed page 的 source/cursor。Pending spool 仍按当前 policy exact replay；容量不足阻止下一页时，有效 ACK 可独立使用 reserve 提交并明确报告 `ack_committed=true`，普通 source failure 仍 rollback ACK。已 admission 物化页的短 reader-position 提交也使用 maintenance reserve；它不允许新增 source observation，仍受真实 filesystem free floor 约束。Voice preparation 因 transaction admission 失败时保留原消息页并报告 not_scheduled，不把附加转写失败变成正文读取失败。阈值是有并发 reservation 的 backpressure，不是 OS quota；空间恢复后 backfill 需明确 resume。

Operator-only `storage explain` 默认 quick：只读既有 DB/WAL metadata 与 content-free memory/history snapshot，不执行全库 COUNT、dbstat 或 observation scan。显式 `--deep` 才运行 tables/layout/observations/sample 阶段；默认 10 秒、至多 25 秒预算覆盖 gate 等待与正在执行的 SQLite SQL，超时／取消返回 partial 与已完成 evidence，tables 可从 last completed object 继续。Sample 限制 examined rows（含 skipped/BLOB），不为了填满 legacy sample 扫完整表；独立 statement 视图不声称全阶段来自同一 snapshot。离线 explain 不打开／初始化／迁移 DB，不生成 history snapshot；旧 daemon 不支持时明确失败。它不压缩、删除或改写历史，exact fields 与命令见 [Operations](OPERATIONS.md#storage-budget-and-maintenance)。

Daemon 另以独立于 `window.db` 的私有 sidecar 最多保存 64 个 content-free UTC-day storage snapshot：固定 owned-byte 分项、message/observation counts 与 timestamp。它不保存正文、label、ID、filename、path 或 credential，不改变 DB schema；启动／重启及跨 UTC 日时更新，同日覆盖。Operator-only `storage explain` 的 history 字段只读已经保存的该 sidecar，并仅在精确第 7／30 天基线存在时报告相应增长差值与线性容量估算，不用更短窗口近似，也不因读取而生成 snapshot。操作和转换边界见 [Operations](OPERATIONS.md#storage-budget-and-maintenance)。

当前 accepted residency enum 与兼容保护状态以 §8.7 和 [Retrieval extension](RETRIEVAL-SPEC.md#sg-058-selective-residency-and-bounded-on-demand-reading) 为准；CLI、默认字节数、schema 实施与安装状态见 [Operations](OPERATIONS.md) 与 [Current state](current-state.md)。

---

## 10. 稳定身份、排序与 Anchor

### 10.1 ID 命名

```text
account_id       wxacct_<opaque>
conversation_id  wxconv_<opaque>
participant_id   wxperson_<opaque>
membership_id    wxmember_<opaque>
message_id       wxmsg_<opaque>
resource_id      wxres_<opaque>
delivery_id      wxdelivery_<opaque>
receipt_id       wxreceipt_<opaque>
```

外部 ID 不应直接暴露原始 wxid、数据库路径、shard path 或本机用户名。

### 10.2 Message identity

优先复用 WGO 已验证的 stable source message identity 逻辑。message ID 必须在：

- cache 重建；
- source snapshot 更新；
- 同一逻辑 shard generation 替换；
- 多次读取；

之间保持稳定，除非无法证明为同一 source row。

### 10.3 Participant identity

`participant_id` 在一个微信账号 namespace 内代表 canonical actor；`membership_id` 代表该 actor 在一个具体 conversation 中的成员关系。至少保证：

- 同一 source internal ID 改昵称、备注或群名片后仍是同一 participant；
- 同名成员不会被合并；
- 私聊 counterpart 与群内 sender 只有在 source key 证据充分时才复用 participant；
- 无稳定 source identity 时，使用 conversation-local participant，并标记 identity confidence；
- 跨账号永不自动合并；
- 跨会话仅凭昵称、头像、公开 handle 或备注相同不得自动合并；
- group card 只改变 membership label，不改变 participant identity。

所有名称都作为 observation 保存。对消息返回时同时计算：

```text
reader_label          # 用户与 reader 最适合使用的称呼
reader_label_source
shown_as nullable     # source/presentation surface 中实际观察到的显示名
shown_as_source nullable
shown_as_temporal_confidence
```

复制界面显示账号昵称而群聊界面显示群名片时，两者都保留；任何一个都不能覆盖另一个。

### 10.4 稳定排序

至少使用：

```text
(sent_at/create_time, source sort_seq, source rowid, source_message_id)
```

同一秒内消息不能只按 timestamp 排序。

### 10.5 Anchor

每条返回消息带 opaque `anchor`。anchor 至少绑定：

```text
account_id
conversation_id
message_id
sort key
schema version
```

anchor 使用本地 secret HMAC 签名。客户端不可篡改 conversation_id 或跨会话使用。

### 10.6 Cursor

cursor 为 opaque signed token，包含：

```text
reader identity binding
account/conversation
position
source receipt / generation binding，或 materialized observation watermark
current-tail projection epoch
policy revision
identity-correction ledger revision（materialized timeline）
issued_at
schema version
```

Live/source-backed cursor 无效、过期、篡改、无法与当前 source reconcile，或 provider/parser interpretation 已改变时返回结构化错误。Materialized timeline cursor 绑定 reader/account/conversation/mode/filter（含 system policy）、projection epoch、policy revision、append-only identity-correction ledger revision、observation repair revision 与首次页面的 observation watermark：watermark 后首次出现的 append 不进入该 traversal，watermark 当时已存在的 row 若随后被 observation correction，或 identity projection 在 ledger 中追加 correction，则 cursor stale。任何 cursor 都不能悄悄从头、从最近消息或另一条 read plane 重启。普通 source append 不得仅因 physical WAL generation 变化就使 timeline/search cursor stale。

---

## 11. Message Contract

### 11.1 单条 detail 消息

```json
{
  "schema": "sightglass.message-detail.v1",
  "message_id": "wxmsg_...",
  "account_id": "wxacct_...",
  "conversation_id": "wxconv_...",
  "anchor": "opaque",
  "sent_at": "2026-09-12T22:31:06+08:00",
  "sender": {
    "participant_id": "wxperson_...",
    "membership_id": "wxmember_...",
    "label": "示例甲",
    "label_source": "sightglass_alias",
    "shown_as": "原账号昵称",
    "shown_as_source": "message_surface",
    "shown_as_temporal_confidence": "exact",
    "identity_state": "stable",
    "is_self": false
  },
  "kind": "text",
  "text": "原始可见正文",
  "reply": null,
  "link": null,
  "forwarded_chat": null,
  "resources": [],
  "state": "present",
  "derivation": {
    "text_kind": "source_visible_text",
    "parser_version": "..."
  },
  "source": {
    "source_message_id": "opaque",
    "observed_at": "ISO-8601",
    "generation_id": "opaque",
    "raw_payload_available": true
  },
  "retrieval": {
    "focus_match": true,
    "context_only": false,
    "matched_participant_ids": ["wxperson_..."]
  }
}
```

`sightglass.message-detail.v1` 是证据完整的单条 contract。它由 `mode="message"` 默认返回，也可由其他 read mode 显式选择 `projection="detail"` 后装入 `sightglass.message-page.v1`。

### 11.2 Compact message batch

Bulk read 默认使用 `sightglass.message-batch.v1`，把共享字段提升到 page level，并用 people table + 固定六列 row 承载消息：

```json
{
  "schema": "sightglass.message-batch.v1",
  "projection": "compact",
  "conversation": {"id": "wxconv_...", "title": "...", "kind": "group"},
  "timezone": "Asia/Shanghai",
  "people": [
    {"id": "wxperson_...", "label": "示例甲", "self": false}
  ],
  "fields": ["id", "when", "who", "kind", "what", "resources"],
  "messages": [
    ["wxmsg_...", "2026-09-12T22:31:06+08:00", 0, "text", "原始可见正文", 0]
  ],
  "markers": {
    "body_truncated": {}
  },
  "page": {
    "has_more_before": false,
    "has_more_after": false,
    "next_cursor": null,
    "delivery_id": null,
    "replayed": false,
    "message_rows_complete": true
  },
  "projection_receipt": {
    "returned_rows": 1,
    "body_complete_rows": 1,
    "body_truncated_rows": 0,
    "serialized_chars": 1234
  }
}
```

`markers.focus`、`markers.context`、`markers.late_arrival`、`markers.state` 与 `markers.body_truncated` 只在相应状态存在时按 row index 稀疏表达；普通 present text row 不复制 retrieval/source/derivation/debug 结构，也不生成 anchor。Compact 的 `resources` 列是 count，不是 descriptor；完整 resource metadata 通过 `wechat_list_resources(message_id)` 取得。

Compact body budget 使用 capped water-filling：短消息优先完整，单条 preview 不超过 policy cap，超长 body 追加 `…` 并在 `body_truncated[index].full_chars` 记录原长度。`page.message_rows_complete` 只表达选中 rows 是否完整；`projection_receipt.body_truncated_rows` 只表达 body preview 是否截断。只有 fixed envelope 本身超限时才按 chronology 减少 rows，并通过 cursor 或 updates ACK continuation 继续。

### 11.3 Message kinds

至少支持：

```text
text
reply
image
sticker
file
link
forwarded_chat
voice
video
system
recalled
location
contact_card
mini_program
unknown
```

未知类型不得丢弃：

```json
{
  "kind": "unknown",
  "wechat_type": 12345,
  "text": "[暂不支持的消息类型]",
  "source": {
    "raw_payload_available": true
  }
}
```

### 11.4 普通文本

- 不修改标点；
- 不删除口癖；
- 不总结；
- 不 trim 正文内部空白；
- sender prefix 与正文分开；
- 群聊 raw sender id 不混进 text。

### 11.5 回复消息

```json
{
  "kind": "reply",
  "text": "当前回复正文",
  "reply": {
    "target_message_id": "wxmsg_... or null",
    "quoted_sender": "用户",
    "quoted_text": "被引用的原文",
    "resolved": true
  }
}
```

无法链接到目标消息时仍保留 quoted content，并返回 `resolved=false`。

### 11.6 转发聊天记录

```json
{
  "kind": "forwarded_chat",
  "forwarded_chat": {
    "title": "...",
    "declared_count": 18,
    "items": [
      {
        "sender": "...",
        "sent_at_text": "...",
        "kind": "text",
        "text": "..."
      }
    ],
    "truncated": false
  }
}
```

### 11.7 System message

默认 `system_policy="include"`。如果客户端选择 omit，page receipt 必须返回 `hidden_system_count`，不能静默消失。

---

## 12. Reader 与更新语义

### 12.1 Reader identity

Reader identity 由 server-side credential 绑定，不能由 tool 参数传入并信任。

设计中的逻辑 profiles（示例身份，不会自动创建）：

```text
demo_reader   # 主 reader；账号全域范围需要 owner 明确授权
codex  # 开发 reader，默认 allowlist / synthetic only
```

当前安装只配置一个 reader。Native 初始化默认选择一个会话的 allowlist；切换到账号全域范围是独立 operator decision。

### 12.2 明确授权后的账号全域 reader 权限建议

```json
{
  "conversation_policy": {
    "mode": "all_except_denylist",
    "denied_conversation_ids": []
  },
  "capabilities": {
    "messages": true,
    "search": true,
    "resource_metadata": true,
    "resource_preview": true,
    "resource_original": true,
    "network_fetch": false,
    "wgo_knowledge": false
  },
  "limits": {
    "max_messages_per_call": 200,
    "max_text_chars_per_call": 120000,
    "max_compact_messages_per_call": 500,
    "max_detail_messages_per_call": 50,
    "max_compact_payload_chars": 180000,
    "max_detail_payload_chars": 120000,
    "max_compact_body_chars_per_message": 4000,
    "max_binary_bytes_per_call": 8388608
  }
}
```

### 12.3 At-least-once delivery

`mode="updates"` 使用 pending delivery：

1. 服务端从 committed cursor 读取；
2. 创建 `pending delivery`；
3. 返回 `delivery_id`、消息和 `next_cursor`；
4. 不立即推进 durable cursor；
5. 下一次 updates 调用携带 `ack_delivery_id`；
6. 服务端原子确认上一 delivery，再读取后续；
7. 未 ack 时重复返回相同 pending delivery。

工具宿主丢失响应时最多重复，不会跳过。Updates 的 committed position 使用 `observation_seq`，不能只使用消息 sent time；晚到或恢复出来的旧时间消息必须标记 `late_arrival=true`。Pending delivery replay 必须返回原批次，不受 alias/parser 更新影响。

### 12.4 Participant-scoped cursor

成员聚焦读取拥有独立 cursor scope：

- `scope_kind="participant"`：单个 participant；
- `scope_kind="filter_set"`：多个 participant 或 participant + query 的稳定 filter digest；
- `scope_kind="conversation"`：完整会话。

`mode="speaker"` 或带 participant filter 的 `updates` 只能推进对应 scope。即使返回时展开了其他人的上下文消息，也不能把这些 context-only 消息算作全会话已读。

### 12.5 Reader cursor 不影响任何外部状态

它不改变：

- 微信 unread count；
- WGO monitor cursor；
- WGO summary bookmark；
- 微信 UI；
- 对方可见状态。

---

## 13. MCP 工具面

普通 materialized message read 在一个短 query-only `WindowDB.read_snapshot` 内完成 target/cursor validation、rows、identity/resource projection、budget 与 receipt，允许并发 writer；timeline progress／voice preparation 在 snapshot 关闭后执行，update seeding 不超过该 frozen watermark。Local context／speaker context 邻居限于 focus 所在的 validated window；未知或不连通区间不作为相邻消息补齐，receipt 的 `continuity` 明确报告 unverified/disjoint_windows/validated_window 和窗口数。

普通 `wechat_read_messages` 默认优先读 current-epoch 已 admission 页。`refresh=true` 是明确的有界 source 重读入口，用于补齐 partial context 或确认当前原话；只支持 recent/context/message/range/speaker，不能与 updates 或 cursor 混用。它复用当前 source/policy/admission 合同，失败不静默返回 stale 页，成功后普通读可重开新观察窗口。

MCP server name：`sightglass`。

首版控制在 8 个工具。所有成功响应为版本化结构化 JSON；图片等媒体可追加 MCP content block，但 JSON descriptor 必须存在。

### 13.1 `wechat_status`

用途：确认服务、数据源、账号、reader、freshness 和 pause 状态。

请求：

```json
{
  "detail": "summary"
}
```

`detail`: `summary | sources | capabilities`

响应：

```json
{
  "schema": "sightglass.status.v1",
  "ready": true,
  "paused": false,
  "reader": {
    "reader_id": "demo_reader",
    "display_name": "授权 reader"
  },
  "source": {
    "state": "complete",
    "fresh_as_of": "ISO-8601",
    "warnings": []
  },
  "accounts": [
    {
      "account_id": "wxacct_...",
      "display_name": "...",
      "active": true
    }
  ],
  "read_plane": {
    "schema": "sightglass.read-plane.v1",
    "live_refresh_available": false,
    "local_cache_reads": true,
    "local_message_reads": true
  },
  "readiness": {
    "indexed_reads": "ready",
    "live_refresh": "degraded",
    "resource_cache": "ready",
    "resource_acquisition": "degraded",
    "voice": "disabled"
  },
  "capabilities": {
    "messages": true,
    "resources": true,
    "wgo_knowledge": false
  }
}
```

顶层 `ready` 表示至少一条已授权 read plane 可用且 reader 未暂停，不再等同于 `source.complete`。各 readiness field 分开表达 indexed message/inbox、live refresh、warm CAS、cold resource acquisition 与 voice configuration；source 不完整不得把仍有效的 materialized projection 或 CAS hit 伪装成不可读，也不得把它们伪装成 live-fresh。

### 13.2 `wechat_find_conversations`

用途：按名称、别名、参与者或最近活动查会话。

请求：

```json
{
  "query": "示例甲",
  "account_id": "wxacct_...",
  "kinds": ["direct", "group"],
  "recent_only": false,
  "limit": 20
}
```

响应候选必须包含稳定 `conversation_id`、kind、当前标题、匹配原因、最近活动时间和 ambiguity 信息。

禁止自动选择第一个模糊匹配。多个合理候选时返回 `ambiguous=true`。


### 13.3 `wechat_find_participants`

用途：在指定会话内按 Sightglass alias、联系人备注、账号昵称、公开 handle、当前/历史群名片、message-surface label 或最近发言解析成员。

请求：

```json
{
  "conversation_id": "wxconv_...",
  "query": "示例甲",
  "active_after": null,
  "detail_level": "labels",
  "limit": 20,
  "cursor": null
}
```

`detail_level`：

- `compact`：只返回 reader label、匹配原因和 ambiguity；
- `labels`：同时返回当前可见 label layers；
- `debug`：仅 operator/显式授权 reader 可用，返回 source key kinds 与 provenance，但仍不暴露原始 internal ID。

响应：

```json
{
  "schema": "sightglass.participant-candidates.v1",
  "conversation_id": "wxconv_...",
  "query": "示例甲",
  "ambiguous": false,
  "total_matches": 1,
  "truncated": false,
  "candidates": [
    {
      "participant_id": "wxperson_...",
      "membership_id": "wxmember_...",
      "label": "示例甲",
      "label_source": "sightglass_alias",
      "labels": {
        "contact_remark": "示例甲",
        "account_nickname": "原账号昵称",
        "current_group_alias": "群名片",
        "public_handle": null
      },
      "matched": {
        "value": "示例甲",
        "kind": "contact_remark",
        "scope": "account",
        "temporal_confidence": "current_only"
      },
      "last_spoke_at": "ISO-8601",
      "resolution_state": "stable",
      "identity_confidence": "exact"
    }
  ],
  "page": {
    "next_cursor": null,
    "truncated": false
  }
}
```

Participant pages use a signed reader/account/conversation/query/policy-bound cursor. The cursor also binds the visible candidate set and ordering；label 或 identity evidence 在翻页期间变化时返回 `CURSOR_STALE`，不得静默重启。`ambiguous` 与 `total_matches` 在每一页描述完整过滤后候选集，`truncated` 与 `page.next_cursor` 只描述当前位置之后是否仍有下一页。

规则：

- 解析范围默认限定在 `conversation_id`；
- 同名候选都必须返回，不能自动选第一个；
- 原始 wxid / internal username 不对 MCP 客户端暴露；
- 公开 handle 只是一种可变 source key/label，不作为唯一身份依据；
- 非好友群成员只要曾在消息中出现，也应当成为候选；
- 复制文本里的显示名只产生 `message_surface` observation，不自动绑定 participant；
- 当前群名片不得冒充历史消息当时的显示名；
- 跨会话 identity linking 不确定时保持分离；
- false merge 时必须提供 operator split/rebind 迁移路径，且不改写 immutable message observations；
- 响应必须包含 participant coverage：当前 roster 是否可读、已观察 sender 的时间范围，以及 `not found` 是否仅表示“当前覆盖内未发现”。

### 13.4 `wechat_read_messages`

统一阅读工具。

请求：

```json
{
  "mode": "recent",
  "conversation_id": "wxconv_...",
  "message_id": null,
  "anchor": null,
  "before": 30,
  "after": 20,
  "limit": 100,
  "direction": "backward",
  "cursor": null,
  "ack_delivery_id": null,
  "participant_ids": [],
  "speaker_view": "only",
  "time_after": null,
  "time_before": null,
  "query": null,
  "projection": "compact",
  "include_resources": "indicator",
  "system_policy": "include",
  "strict": true,
  "voice": null
}
```

`mode`：

```text
recent
context
updates
range
message
speaker
```

规则：

- `recent` 需要 conversation_id；
- `context` 需要 anchor 或 message_id；
- `updates` 需要 conversation_id，可带 ack_delivery_id；
- `range` 需要 conversation_id + cursor/time boundary；
- `message` 需要 message_id；
- `speaker` 需要 conversation_id + 至少一个 participant_id；
- `speaker_view="only"` 只返回匹配成员本人消息；
- `speaker_view="with_context"` 为每个匹配消息展开 before/after，重叠窗口必须合并；
- `recent`、`range`、`speaker`、`updates`、`context` 默认 `projection="compact"`；`message` 默认 `projection="detail"`；
- compact 的 `include_resources` 为 `none | indicator`，默认 `indicator`；detail 为 `none | metadata`，默认 `metadata`；
- compact read 上限 500 rows，detail 上限 50，message mode 固定 1；省略 `limit` 时按 projection 选择有效默认；
- `limit` 在 speaker 模式下计算 focus matches 数量，不计算附带 context，但总返回消息仍受服务端预算限制；
- speaker 模式覆盖全部消息类型，纯图片或文件消息也算该成员发言；
- `query` 可进一步限定该成员发言；无 query 时可按时间范围读取其全部发言；
- speaker cursor 按 focus message 位置分页，不以最后一条 context-only 消息推进；
- participant-filtered updates 使用 participant/filter-set scoped delivery，不推进全会话 cursor。
- `recent`、`context`、`message`、`range`、`speaker` 在 conversation state 与 message row 都属于 current projection epoch 时，默认从 schema-v5 `window.db` materialized plane 返回；只有 cold miss、stale semantic epoch 或不兼容的 live cursor 才进入既有 source-backed path。`updates` 继续使用其独立 observation/delivery/ACK contract；search 继续对 index candidate 做 current-source validation。
- Materialized page 的 `source_receipt.served_from="window_db"`、`freshness.state="bounded_stale"` 且 `live_refresh_confirmed=false`，并带 projection epoch、observation watermark、coverage 与已记录的 live error。它不能声称本次调用重新验证了 live source。
- `voice` 取 `auto | cached | off | null`，缺省继承本地配置的默认 policy；未启用 voice 的安装一律等价于 `off`；非法值返回 `QUERY_INVALID`。它只在**最终交付行集合固定之后**生效：`auto` 会用实际交付的 voice 资源建/复用一个小批次（首步最多 3 项，由配置注入预算），`cached` 只挂已有缓存结果并刷新 cache 命中，`off` 不做任何事。status/find/inbox/search/list/metadata 入口不会自动准备语音，reader 必须已有 `resource_preview` 能力，暂停/拒绝在读取与返回两点都会复验。

响应：

```json
{
  "schema": "sightglass.message-batch.v1",
  "projection": "compact",
  "mode": "recent",
  "conversation": {
    "id": "wxconv_...",
    "title": "...",
    "kind": "group"
  },
  "timezone": "Asia/Shanghai",
  "people": [],
  "fields": ["id", "when", "who", "kind", "what", "resources"],
  "messages": [],
  "markers": {"body_truncated": {}},
  "focus": {
    "participant_ids": [],
    "speaker_view": null,
    "matched_message_count": 0,
    "context_message_count": 0
  },
  "page": {
    "has_more_before": true,
    "has_more_after": false,
    "next_cursor": "opaque",
    "delivery_id": null,
    "replayed": false,
    "message_rows_complete": true
  },
  "projection_receipt": {
    "returned_rows": 0,
    "body_complete_rows": 0,
    "body_truncated_rows": 0,
    "serialized_chars": 1234
  },
  "source_receipt": {
    "complete": true,
    "fresh_as_of": "ISO-8601",
    "inventory_digest": "opaque",
    "returned_count": 0,
    "hidden_system_count": 0,
    "warnings": []
  }
}
```

显式 detail response 使用 `sightglass.message-page.v1` envelope、`projection="detail"` 与 `sightglass.message-detail.v1` messages，并保留既有 `page.truncated` message-boundary 语义。Compact response 不用 `page.truncated` 混合表示 row 与 body 两类截断。

当 `voice` 生效且交付行里存在 message-bound voice 资源时，compact 与 detail page 都额外带一个页级 `voice` sidecar（`sightglass.voice-sidecar.v1`，只读、稀疏、不改变 `fields` 六列与 `messages` 结构）：

```json
{
  "voice": {
    "schema": "sightglass.voice-sidecar.v1",
    "policy": "auto",
    "state": "prepared",
    "derivation": {"kind": "derived_transcript"},
    "reading_token": "voice_...",
    "expires_at": "ISO-8601",
    "fields": ["message_id", "resource_id", "state", "text"],
    "items": [],
    "items_complete": true,
    "text_inline": true,
    "selected_count": 2,
    "coverage": {"selected": 2, "ready": 0, "pending": 2, "not_scheduled": 0,
                 "blocked": 0, "failed": 0, "empty": 0, "cancelled": 0},
    "excluded": {"unreadable": 0, "unknown_revision": 0},
    "guide": "read committed transcripts with wechat_read_transcripts(...)"
  }
}
```

- `state` 取 `prepared`（有未完成工作）、`cached`（全部命中缓存）、`cached_only`（cached policy 未命中）、`terminal`（已无待处理工作，但至少一项为 blocked / failed / cancelled / not_scheduled；具体类别以 coverage 为准）、`unreadable`（无可读本地 voice payload，零任务）。summary 不得把 `failed>0, blocked=0` 的批次误称为 `blocked`。
- `items` 是稀疏行级映射，只列已有 committed 转写的结果行；转写文本只出现在这里（或经 reading token 补读），**不**写进 `messages` 的正文，也不在每行重复 engine/version receipt。超出内联上限或页预算时文本省略、`text_inline=false`、`items_complete=false`，`guide` 指向 `wechat_read_transcripts` 补读；原始消息行不会被裁掉。
- sidecar 为空或不存在时（语音入口关闭、无 voice 行、裁剪掉的候选、reader 无资源能力）response 与现状逐字节一致。
- `updates` 路径在 `delivery_store.write` **之前**冻结 sidecar：pending delivery 的 replay 返回完全相同的 payload，不重新建批、不刷新 job、不按当前识别状态改写正文；voice 完成不推进 message cursor，也不改变 ACK 语义。

### 13.5 `wechat_search_messages`

请求：

```json
{
  "query": "真人自拍",
  "account_id": "wxacct_...",
  "conversation_ids": [],
  "participant_ids": [],
  "sender_query": null,
  "after": null,
  "before": null,
  "limit": 50,
  "strict": true
}
```

搜索语义：

- 默认 whitespace terms 为 AND；
- quoted phrase 为精确短语；
- 支持短中文词；
- 先候选召回，再 canonical validation；
- 2026-10-03 接受的 request-lifecycle extension：daemon 首页先返回 `sightglass.search-preparation.v1` preparing + signed token；同参数以 `reading_token`（或 `cursor`）轮询，准备中不等于空 hits。后台有界扫描与 conversation admission checkpoint 可恢复，未完成 conversation 重启后重新验证。token 绑定 reader/policy/source/store/request digest，pause/deny/replacement 失效；query text 不入 job。细节以 [MCP contract](MCP-CONTRACT.md#asynchronous-search-preparation) 为准，schema v9 不变；
- 准备完成并经过新的 strict current-source validation 后返回 `sightglass.search-results.v2`，复用 compact people table、六列 hit rows、page-level conversation table、sparse match markers 与 coverage receipt；
- search hit 不重复 detail anchor/source/derivation；需要完整证据时按 opaque message ID 调用 `mode="message"`；
- `participant_ids` 是 canonical sender filter；`sender_query` 仅是便利入口，若有歧义必须返回 participant candidates；
- `query` 可以为空，但仅允许在提供 participant_ids 且给出有界时间范围时使用，此时等价于“列出该成员在此范围内的全部发言”；
- sender filter 适用于非文本消息；图片/文件等无正文消息仍应返回其 descriptor；
- 不在工具中调用 AI 总结。

### 13.6 `wechat_list_resources`

请求：

```json
{
  "message_id": "wxmsg_..."
}
```

响应每个 resource：

```json
{
  "resource_id": "wxres_...",
  "kind": "image",
  "mime_type": "image/jpeg",
  "original_name": null,
  "declared_size": 842391,
  "availability": "local_available",
  "preview_available": true,
  "original_available": true,
  "source_message_id": "wxmsg_..."
}
```

### 13.7 `wechat_read_resource`

请求：

```json
{
  "resource_id": "wxres_...",
  "mode": "preview",
  "page": null,
  "start_line": null,
  "end_line": null,
  "max_bytes": 4194304
}
```

`mode`：

```text
metadata
preview
original
text
page
members
```

`metadata` 对本地可读的 voice original 不做转码或识别，直接返回 audio descriptor（例如 `format="silk"`、`mime_type="audio/silk"`）；它不得因为 generic dispatcher 未覆盖 audio kind 而返回 `RESOURCE_UNSUPPORTED`。

响应 JSON descriptor 包含：

- 来源消息；
- resolution path；
- MIME sniff 结果；
- returned bytes / chars；
- 是否截断；
- derivation；
- warning。

本机真实路径永不返回给 reader。

voice kind 的 `mode="text"` 是唯一的派生分支：它走**同一个** `VoiceService`、同一 selection 去重与同一结果缓存，不另造同步识别通道。descriptor 的 `derivation.kind="derived_transcript"`、`resolution.variant="derived_transcript"`，并带 `sightglass.voice-transcript.v1` 的 `transcript` 状态：

- `state="ready"`：返回转写文本（text content block），`media.mime_type="text/plain"`；
- `state in {"pending","not_scheduled"}`：不返回正文，但给出 `reading_token`、`coverage` 与经 `wechat_read_transcripts` 续读的 `guide`；
- `state in {"blocked","failed","unsupported"}`：缺本地 payload key、识别被永久阻塞或缺少 recorded revision evidence 时如实报告，并附 content-free `error_code` 与 `transcript_*` / `voice_payload_unavailable` warning；
- 该分支不从当前 source 重读原始 payload（warning `derived_transcript_source_not_reread`），但仍要求 resource 存在、resolver active 且 reader 具备 `resource_preview` 能力与当前会话权限；voice 未启用时维持既有 `RESOURCE_UNSUPPORTED` 行为。

### 13.8 `wechat_search_resource_text`

请求：

```json
{
  "resource_id": "wxres_...",
  "query": "atomic publish",
  "limit": 20
}
```

仅适用于已安全提取文本的 PDF、TXT、Markdown、JSON 和代码文件。返回命中片段、页码或行号，不返回整份文件。

### 13.9 `wechat_read_transcripts`

请求：

```json
{
  "reading_token": "voice_...",
  "cursor": null,
  "wait_ms": 8000
}
```

`reading_token` 是服务端签发的 opaque 批次句柄（`batch_id`），从不来自 URL、路径或 source key。`reader_id`、`account_id`、`account_binding_id` 由 daemon 进程配置与 `window.db` 解析，工具参数无法伪造；不属于当前 reader/installation binding/active account 的 token 一律 fail closed。

响应是 `sightglass.voice-page.v2`：`fields=[message_id,resource_id,ordinal,state,text,error_code]`、八项 `coverage`、`processing_complete`、`text_coverage_complete`、`has_more_results_now`、signed `next_cursor` 与 `expires_at`，并标记 `derivation.kind="derived_transcript"`。每页最多交付一条不可变事件，cursor 只回放该 scope 尚未见过的事件，因此乱序完成的结果仍各交付一次；item 是事件提交时的 immutable state transition，coverage 则是响应组装时的 current aggregate snapshot，两者时态不同且必须在 contract 中明确。`error_code` 只在对应 message/resource 的 terminal event 上提供 content-free 原因；`text` 是派生识别结果，既不冒充原消息，也不替代 message-bound `audio/silk` 资源。`failed` 表示 daemon 已耗尽 bounded transient retry budget，在该 reading batch 内为终态；重复读取同一 token 不重启任务，reader MCP 当前不提供显式 retry mutation。

`wait_ms` 可选，缺省 8000，服务端上限 15000，`0` 表示不等待，负数或非整数为 `QUERY_INVALID`；生效上限恒短于 25 秒工具 deadline。voice page 同时带 `sightglass.voice-wait.v1`：`state` 取 `delivered`/`complete`/`disabled`/`unavailable`/`capacity_exhausted`/`woken`/`timeout`/`aborted`，并报告 `requested_ms`、`elapsed_ms`、`active_waiters`、`max_waiters`、`waiter_available`、`voice_worker_enabled`。pending、timeout、容量耗尽与未配置识别器都是正常结果，不是 `isError`，也不伪装成 pending 条目。

等待期间不持有 read gate、foreground claim 或 tool slot，系统级最多 2 个 waiter；`daemon.status`、summary status、其他正文工具与 operator pause/deny/reload 仍可立即取得，并会唤醒 waiter 让其在新 policy 下复读。调用方断开只释放自己的 waiter。

失败沿用 `sightglass.error.v1`：未知/越界 token 为 `CURSOR_INVALID`，过期批次为 `CURSOR_STALE`，已 pause 为 `SERVICE_PAUSED`，会话已被 deny 为 `POLICY_DENIED`，参数非法为 `QUERY_INVALID`；错误 envelope 不带 wait block。

---

## 14. 附件与资源系统

### 14.1 资源解析顺序

```text
1. 私人 resource cache / 已生成 preview
2. WGO CAS（可选 adapter）
3. 微信本地已落地文件
4. 微信本地可解码数据
5. metadata-only / unavailable
```

默认不进行远程网络下载。

`wechat_read_resource` 在任何 source hydration 前先解析 canonical resource row，重查当前 reader 的 owning-conversation policy 与 active resolver。若所需 original/thumbnail/derived source binding 已在 private CAS，读取必须保持本地：每次仍验证 object path、mode、single-link、size 与 digest；processor 在 source snapshot 外运行；短 admission transaction 再比较 captured resolver revision 后才提交新 derivative binding。Resolver/policy/revision 改变或 CAS 损坏必须 fail closed，不能因 cache hit 放宽完整性，也不能在 daemon 已选择 local-only route 后静默回落到 provider。Cold miss 仍使用既有 source-backed path，dependency-scoped cold acquisition 属于后续 tranche。

### 14.2 Availability states

```text
metadata_only
local_available
archive_available
preview_only
not_downloaded
missing
cleaned_by_wechat
key_missing
unsupported
decode_failed
blocked_by_policy
```

### 14.3 图片

M2 必须支持：

- 普通图片缩略图与原图；
- WGO 已有 V2 图片解码逻辑；
- MIME sniff；
- 尺寸、像素和 bytes 上限；
- corrupted image fail closed；
- EXIF 不默认返回；
- preview 可重新编码为安全 PNG/JPEG；任一边最长 2048 px，原图较小时不得 upscale；`preview_available` 表示可立即读取或在本地派生，不只表示已有 cached derivative，生成 recipe 变化时不得复用旧 recipe 的缓存；
- animated image 首版返回静态 preview + original metadata。

### 14.4 PDF

M2 必须支持：

- metadata；
- 页数；
- 文本提取；
- 指定页渲染；
- 按页读取；
- 搜索提取文本；
- encrypted / corrupted / oversized 状态；
- 不一次性把整份大 PDF 注入上下文。

### 14.5 文本、Markdown、JSON、代码

M2 必须支持：

- 安全编码探测；
- 行区间读取；
- 文本搜索；
- char / line / byte limits；
- binary masquerading as text 检测；
- 不执行代码。

### 14.6 链接卡片

保留微信消息自带的：

```text
title
description
url
source/app name
cover metadata
```

默认不访问 URL。URL 凭据、query secrets 和 fragment secrets 在日志中必须脱敏；exact URL 默认只在本地受控数据中保存；owner 已明确授权 `wechat_find_links` 与 `wechat_retrieve` 返回完整 observed raw/normalized URL（含 credentials、port、query、fragment），其他消息投影仍脱敏。该例外不允许 URL fetch，也不允许 URL 进入日志、access receipts 或 Git。

### 14.7 转发聊天记录

优先作为结构化 message payload，而非普通文件。保留 item 顺序、sender、时间文本、kind、总数与 truncation 状态。

### 14.8 语音与视频

M2 只要求 metadata。后续：

- 语音：原始音频 + 本地转写，明确 `derived_transcript`；
- 视频：metadata + cover frame + bounded frame extraction；
- 不把转写当成 source-visible original text。

### 14.9 Office 与压缩包

后续阶段：

- Office：安全文本提取和预览，禁用宏；
- archive：只列目录，防 path traversal、nested bomb、极端 compression ratio；
- 不执行成员文件；
- 选择性读取单个安全文本成员。

---

## 15. 权限与隐私

### 15.1 权限维度

每个 reader 独立配置：

```text
conversation scope
message read
cross-conversation search
resource metadata
resource preview
resource original
network fetch
WGO knowledge
maximum output budgets
```

### 15.2 Conversation policy

支持：

```text
allowlist
all_except_denylist
```

deny 的会话：

- 不出现在 find results；
- 不参与 search；
- 不能通过 message_id、anchor 或 resource_id 绕过；
- WGO knowledge adapter 命中时也必须重新执行 conversation policy。

### 15.3 总暂停开关

`paused=true` 时所有 MCP tools 返回 `SERVICE_PAUSED`。status 可读取暂停状态，但不能通过 MCP 恢复。

恢复只能在本地 CLI / control plane 进行。

### 15.4 数据出境最小化

每次调用只返回当前任务需要的消息窗口与资源。默认：

- recent 100 条以内；
- context before 30 / after 20；
- search 50 hits；
- 单次文本 120k chars；
- 单次 binary 8 MiB；
- 超限返回 truncation 与 continuation cursor。

### 15.5 审计收据

记录：

```text
reader_id
tool
conversation_id opaque
message count
resource count
byte count
time range
outcome
warnings
duration
```

不记录：

```text
message body
contact display names
original filename
exact URL
local filesystem path
raw attachment content
```

---

## 16. Threat Model

### 16.1 Prompt injection

消息与附件中的“忽略之前指令”“打开另一个群”“发送数据”等文字只作为内容返回。Resource parser 不得触发 MCP 调用或权限变化。

MCP 响应应把 source content 放在明确字段中，不拼进 server instructions。

### 16.2 文件攻击

必须覆盖：

- MIME spoof；
- path traversal；
- symlink / hardlink escape；
- oversized image；
- corrupted image；
- PDF bomb；
- zip bomb；
- deeply nested archive；
- Office macro；
- malicious filename；
- unsupported codec。

### 16.3 本地权限

- daemon socket mode `0600`；
- data dir mode `0700`；
- DB/cache objects mode `0600`；
- auth secrets 存 Keychain 或等价安全存储；
- token 只存 hash；
- IPC 验证 peer / token；
- server 不监听公网；
- logs 不泄露 key、path、正文；
- `doctor` 检查并报告承载 `window.db` 与 resource cache 的 volume encryption 状态；不把文件权限误当作磁盘加密。

### 16.4 Cursor / ID 越权

所有 anchor、cursor、resource_id 查询必须再次做 reader policy check，不能因为 ID 已知就绕过权限。

### 16.5 TOCTOU

资源读取：

- open with no-follow where available；
- verify regular file；
- read via opened file descriptor；
- validate size/MIME after open；
- preview generation 使用临时文件 + atomic publish；
- local object 以 digest 验证。

---

## 17. Source 完整性与 Freshness

### 17.1 绝不把 incomplete 当成 empty

以下状态必须返回错误或 warning：

```text
source directory unavailable
key missing
message shard missing
cache-only without proof
snapshot failed
WAL reconstruction failed
decode failed
generation changed
FTS coverage unknown
resource not locally available
```

### 17.2 Source receipt

每个 message page / search response 都带：

```json
{
  "complete": true,
  "fresh_as_of": "ISO-8601",
  "inventory_digest": "opaque",
  "generation_set_digest": "opaque",
  "coverage": {
    "conversation": "complete",
    "time_range": "complete"
  },
  "warnings": []
}
```

### 17.3 Search coverage

搜索必须说明：

- 使用哪个索引；
- 是否 canonical validated；
- 搜索时间范围；
- 是否覆盖所有授权会话；
- 是否存在未索引 source shard。

---

## 18. 错误 Contract

统一错误：

```json
{
  "schema": "sightglass.error.v1",
  "ok": false,
  "code": "SOURCE_INCOMPLETE",
  "message": "微信消息源当前不完整，读取未执行。",
  "retryable": true,
  "details": {
    "warning_codes": ["source_key_missing"]
  }
}
```

错误码至少包括：

```text
SERVICE_PAUSED
SOURCE_NOT_CONFIGURED
SOURCE_PERMISSION_DENIED
SOURCE_KEY_MISSING
SOURCE_INCOMPLETE
SOURCE_GENERATION_CHANGED
SOURCE_SNAPSHOT_FAILED
SOURCE_MESSAGE_DECODE_FAILED
ACCOUNT_NOT_FOUND
CONVERSATION_NOT_FOUND
CONVERSATION_AMBIGUOUS
PARTICIPANT_NOT_FOUND
PARTICIPANT_AMBIGUOUS
PARTICIPANT_OUT_OF_SCOPE
MESSAGE_NOT_FOUND
POLICY_DENIED
CURSOR_INVALID
CURSOR_STALE
DELIVERY_ACK_INVALID
RESOURCE_NOT_FOUND
RESOURCE_UNAVAILABLE
RESOURCE_TOO_LARGE
RESOURCE_UNSUPPORTED
RESOURCE_DECODE_FAILED
RESOURCE_BLOCKED
QUERY_INVALID
OUTPUT_BUDGET_EXCEEDED
INTERNAL_ERROR
```

对 reader 返回 privacy-safe message；详细 stack trace 只进入本地受限日志，并继续执行正文/path/URL redaction。

---

## 19. 日志与可观察性

### 19.1 日志原则

- MCP stdout 只写 protocol；
- app logs 写 stderr 或受限文件；
- JSON structured logs；
- 默认 INFO 不含正文；
- debug 也不应默认写消息内容；
- key、token、path、exact URL、联系人名统一脱敏。

### 19.2 Health 指标

至少可查询：

```text
daemon uptime
source state
last successful source refresh
active account
window.db schema/version
pending deliveries
resource cache size
last error code
WGO adapter state
MCP bridge connected state
```

### 19.3 Receipts

每个 tool call 生成本地 access receipt。失败调用也记录 outcome 与 error code，但不记录参数中的自然语言正文。

---

## 20. 测试策略

所有自动测试使用 synthetic data。未经用户明确授权，Codex 不得读取真实微信数据库、真实 key、真实消息或真实附件。

### 20.1 Unit tests

- plain text 逐字保留；
- group sender prefix 分离；
- private sender self/counterpart；
- group outgoing message without sender prefix resolves to account self；
- reply parsing；
- link/file/image/sticker/system/unknown parsing；
- forwarded chat order；
- stable ID；
- opaque anchor HMAC；
- cursor tamper；
- reader policy；
- error mapping；
- audit redaction。

### 20.2 Source integration tests

Synthetic SQLite fixtures 覆盖：

- group + direct；
- 多 message shards；
- 同秒多消息；
- rowid/sort_seq tie break；
- generation replacement；
- missing shard；
- key missing；
- corrupt zstd；
- snapshot change during traversal；
- source FTS candidate + canonical validation；
- short Chinese query；
- contact rename / group rename；
- group member nickname rename；
- contact remark / account nickname / group card / public handle 同时存在；
- copied message-surface label differs from group card；
- forwarded/nested sender labels remain unresolved and do not merge outer participants；
- public handle changes or reuse never changes canonical participant；
- global/scoped source-key partial unique indexes handle NULL correctly；
- source path move does not silently define account identity；
- reader-timezone day boundary and system timezone change；
- current group card is not retroactively asserted for old messages；
- stable source ID survives every label change；
- non-contact group member discovered from message sender；
- two members with the same display name remain distinct；
- unknown message type。

### 20.3 Reader tests

- recent pagination；
- context before/after；
- context at conversation boundary；
- updates first delivery；
- no ack → exact replay after alias/parser/source changes；
- late-arriving message with older sent_at is delivered by observation sequence；
- bounded absence never creates deletion/recalled state；
- ack → advance；
- stale cursor；
- policy change invalidates pending delivery；
- denylist blocks message/resource direct ID access；
- participant resolution by current alias and historical alias；
- alias precedence returns separate `label` and `shown_as`；
- public handle match never overrides exact internal source identity；
- alias-only imported transcript remains unresolved until canonical reconciliation；
- ambiguous same-name participant returns candidates；
- speaker only view；
- speaker with-context view and overlapping-window merge；
- reply target outside participant filter remains visible as quoted context；
- image/file-only messages count as participant utterances；
- participant-filtered updates do not advance conversation cursor；
- output truncation and continuation。

### 20.4 Resource tests

- image preview/original；
- V2 encrypted image fixture；
- bad MIME extension；
- corrupted image；
- oversized image；
- PDF text/page/search；
- encrypted PDF；
- malformed PDF；
- text line range；
- invalid encoding；
- binary masquerading as text；
- path traversal；
- symlink escape；
- no implicit network request；
- resource identity remains stable if parser discovery order changes。

### 20.5 MCP contract tests

- tool schema snapshot；
- versioned response；
- structured errors；
- media descriptor + content block alignment；
- reader identity cannot be spoofed by args；
- participant identity cannot be spoofed across conversation scope；
- stdout contains no stray logs；
- service paused behavior。

### 20.6 WGO adapter tests

- WGO absent → core product fully functional；
- WGO CAS hit；
- WGO knowledge hit respects reader scope；
- missing source linkage explicit；
- adapter failure does not break raw WeChat reading。

### 20.7 Security tests

- prompt injection remains plain data；
- malicious filenames；
- zip path traversal/bomb when archive support lands；
- log redaction；
- token hash storage；
- socket permissions；
- cursor/resource ID cross-reader replay denied。

---

## 21. 分阶段实施

### M0 — Audit 与 contract freeze

交付：

- 审查 WGO 当前 main，并记录 exact commit SHA、license/NOTICE 与文件清单；
- `docs/WGO-REUSE-MAP.md`；
- source-neutral / WGO-specific 边界；
- contracts 与 error codes；
- synthetic fixture plan；
- repo skeleton；
- 许可证/provenance 记录。

Gate：不读取真实微信数据，不做 deployment，不改 WGO 主仓。

### M1 — Source + text reading foundation

交付：

- DirectWeChatSourceProvider 最小实现；
- source health / snapshot；
- window.db v1（包含 stable/mutable identity 分层、partial unique indexes、observation sequence、timeline/update cursor 分离与 pending-delivery replay 所需 schema）；
- conversation discovery 与 coverage receipt；
- participant/member indexing、account self identity、multi-layer label observations、alias history 与 conversation-scoped resolution；
- explicit UTC/timezone semantics；
- message parser；
- stable IDs；
- recent / message / range / basic speaker-only reading；
- `wechat_status`、`wechat_find_conversations`、`wechat_find_participants`、`wechat_read_messages`；
- synthetic integration tests。

Gate：群聊、私聊、self/outgoing、多 shard、同秒排序、incomplete fail-closed、同名成员消歧、mutable handle 不参与 canonical linking、群昵称/备注/账号昵称变化后身份连续、`label` 与 `shown_as` 分离、coverage 明确、speaker-only reading 全部通过。

### M2 — Context、search、updates

交付：

- context window；
- speaker with-context view；
- participant-scoped search and updates；
- canonical search；
- signed cursor；
- reader profile；
- observation-sequence updates、late-arrival handling；
- exact pending delivery replay + idempotent ack；
- denylist/allowlist；
- access receipts；
- `wechat_search_messages`。

Gate：真实交互语义在 synthetic E2E 中成立：recent → participant resolve → speaker-only / with-context → participant updates → ack，且 conversation cursor 不被误推进。

### M3 — 图片、PDF、文本附件

交付：

- stable source_resource_key / resource binding；
- resource metadata；
- image preview/original；
- PDF page/text/search；
- text/code line read/search；
- resource cache；
- `wechat_list_resources`、`wechat_read_resource`、`wechat_search_resource_text`；
- 安全测试。

Gate：消息与资源始终通过 message_id/resource_id 绑定；无悬空“最近几张图”接口。

### M4 — Long-running daemon 与 control plane

交付：

- `sightglassd`；
- Unix socket；
- thin MCP bridge；
- operator CLI；
- pause/resume；
- local config/Keychain；
- cache status/cleanup；
- local alias set/unset 与 identity correction ledger（merge/split/rebind/rollback）；
- process identity / restart recovery。

Gate：MCP 重启不重新扫描/解密微信源；daemon crash 后可恢复 pending delivery。

### M5 — WGO adapters

交付：

- WGO CAS lookup；
- knowledge search/get event；
- source provenance link；
- adapter status；
- no-WGO fallback。

Gate：WGO selection 不限制原微信阅读；reader policy 始终优先。

### M6 — 真实本机 canary 与 UX

在用户明确授权后：

- isolated test account / selected conversation canary；
- local MCP client；
- 授权 reader profile；
- access receipt review；
- 图片/PDF真实读取；
- permission recovery；
- final operator docs。

真实数据验收必须与源码测试分开记录。

### M7 — 后续能力

- 语音本地转写；
- 视频 cover/frames/transcript；
- Office；
- archive members；
- menu bar UI；
- secure remote bridge；
- explicit watch/notification lane；
- other sources beyond WeChat。

---

## 22. 产品验收场景

### 场景 A：最近现场

用户：

> 看看示例甲刚才在说什么。

通过条件：

- 找到正确会话或返回明确候选；
- 读取最近消息；
- sender/time/order 正确；
- 有附件时返回 resources；
- source receipt 表明 complete / incomplete；
- 不需要用户复制正文。

### 场景 B：回到前后文

用户：

> 她那句前面在回什么？

通过条件：

- 使用 message anchor；
- 返回 before/after；
- 不跨错会话；
- 引用关系可见；
- boundary / truncation 明确。

### 场景 C：继续看回复

用户：

> 她又回了。

通过条件：

- 从授权 reader committed cursor 开始；
- 若上一阅读焦点是某个 participant，则使用 participant-scoped cursor；否则使用 conversation cursor；
- pending delivery 可 replay；
- ack 后只推进对应 cursor scope；
- 不影响微信/WGO状态。

### 场景 D：图片

用户：

> 看看她刚才发的那张图。

通过条件：

- 图片绑定具体 message；
- 先 preview；
- 可按需 original；
- 返回发送者、时间与上下文；
- 资源不存在时解释具体状态。

### 场景 E：PDF

用户：

> 她发的 PDF 第三页在说什么？

通过条件：

- 定位 PDF resource；
- 只读取第三页文本/渲染；
- 明确页码和 derivation；
- 不把整份大 PDF 一次塞入上下文。

### 场景 F：搜索旧原话

用户：

> 她昨天是不是说过初版只支持本地文件？

通过条件：

- 搜索候选；
- canonical validate；
- 返回命中原文 + anchor；
- 可继续 read context；
- coverage receipt 说明时间与会话范围。


### 场景 G：群成员聚焦

用户：

> 只看示例甲今天在这个群里说了什么。

通过条件：

- 先在指定群内解析稳定 participant identity；
- 同名成员时返回候选，不自动猜测；
- 返回示例甲本人发送的所有消息类型，包括纯图片、文件和回复；
- `speaker_view="only"` 时不混入其他人的普通消息；
- `speaker_view="with_context"` 时邻近消息明确标记为 context-only，重叠窗口不重复；
- 可进一步按关键词搜索她的发言，并从命中 anchor 展开现场；
- 该读取与后续 participant updates 使用独立 cursor，不推进整个群聊的 授权 reader cursor。

### 场景 H：权限

- denylist 会话无法被 find/search/direct ID/resource ID 访问；
- pause 后所有读取停止；
- Codex reader 不继承授权 reader 权限；
- 日志无正文与本机路径。

---

## 23. 完成定义（Definition of Done）

一个 milestone 只有在以下条件全部满足时才算完成：

1. contract 已文档化并版本化；
2. synthetic unit/integration/security tests 通过；
3. `compileall` / type checks / lint / diff check 通过；
4. 无真实微信数据、key、路径、联系人、消息或附件进入 repo、test artifact、日志或 PR；
5. source incomplete 不会被报告为空；
6. reader policy 有直接 ID 绕过测试；
7. MCP stdout 无普通日志；
8. 所有新依赖与许可证记录完整，WGO 复用记录绑定 exact commit SHA；
9. Codex 报告已完成、未完成、风险与下一 gate；
10. 未经明确指令，不 merge、不 deploy、不做真实数据 canary。

---

## 24. Codex 实施约束

### 24.1 首次执行范围

首次把本 spec 交给 Codex 时，只执行 **M0 + M1**。

不要一次性实现所有附件、daemon、WGO adapter 和真实部署。先把 source/read contract、稳定 ID、文本阅读与 synthetic tests 做扎实。

### 24.2 Codex 开工流程

1. 读取本 spec；
2. 记录 WGO exact commit SHA、license/NOTICE 与审查文件清单，再审查该冻结 baseline；
3. 写 `WGO-REUSE-MAP.md`；
4. 建新 repo/branch skeleton；
5. 冻结 v1 contracts；
6. 建 synthetic fixtures；
7. 实现 M1；
8. 跑完整测试；
9. 生成 handoff report；
10. 停在 gate，不 merge、不 deploy。

### 24.3 禁止事项

- 不接触真实微信数据；
- 不读取真实 key/config/cache；
- 不修改 WGO 生产状态；
- 不重签微信；
- 不安装 LaunchAgent；
- 不打开公网端口；
- 不调用付费模型；
- 不把 WGO monitor/digest 代码拖进核心；
- 不以一个巨型 `mcp_server.py` 交付；
- 不用“先返回空列表”掩盖 source error；
- 不自动简化为“WGO 再加几个工具”。

### 24.4 Handoff report 模板

```markdown
# Handoff

## Scope completed

## Architecture / contracts

## WGO reuse map

## Files changed

## Tests and exact results

## Privacy / real-data boundary

## Known gaps

## Risks / decisions needed

## Recommended next milestone
```

---

## 25. 直接给 Codex 的启动提示

```text
请完整阅读仓库中的《Sightglass 产品与工程规格 v0.4》。

本次只执行 M0 + M1：
1. 冻结并记录 IndelibleVivi/we-groupchat-obsidian 当前 exact commit SHA、license/NOTICE 与审查文件清单，产出 WGO-REUSE-MAP.md；
2. 建立独立 `sightglass` repo/branch skeleton；
3. 冻结 source/message/participant/membership/identity-label/resource/receipt/error contracts；
4. 建立 synthetic SQLite fixtures；
5. 实现 source health、coverage-aware conversation discovery、account self identity、participant/member indexing、stable principal keys 与 mutable label evidence 分层、多层 label observations、alias history、conversation-scoped participant resolution、explicit timezone semantics、stable IDs、window.db v1、message parser，以及 recent/message/range/basic speaker-only 文本阅读；
6. 暴露 wechat_status、wechat_find_conversations、wechat_find_participants、wechat_read_messages 的最小 MCP contract；
7. 完成群聊、私聊、多 shard、同秒排序、generation change、missing shard、unknown message type、同名成员消歧、群昵称/备注/账号昵称/公开 handle 分层、复制界面显示名差异、非好友群成员和 policy 基础测试。

关键边界：
- 这是独立产品，不是给 WGO 增加几个工具；
- 复用 WGO 的 source 能力与测试经验，不依赖 monitor selection、topic/event、Digest、summary bookmark、Obsidian 或 AI provider；
- 所有数据和测试必须 synthetic；
- 不读取真实微信、真实 key、真实消息、真实附件或真实本机配置；
- 不 merge、不 deploy、不做真实 canary；
- source incomplete 必须 fail closed；
- 普通文本逐字保留；
- internal stable source identity 决定 canonical participant；公开 handle 属于 mutable evidence，不能进入 canonical unique map；昵称、备注、群名片和 message-surface label 只用于召回与显示；
- SQLite nullable scope 使用 partial unique indexes，不能依赖包含 NULL 的普通 UNIQUE；
- account self / group outgoing / direct incoming-outgoing 必须显式建模；
- `window.db` 预留 observation sequence、timeline/update cursor 分离与 exact pending-delivery replay schema；
- source 覆盖不完整时，not-found 必须附 coverage，不能断言对象不存在；
- 不凭同名、头像或文本风格自动合并成员；
- basic speaker-only reading 覆盖纯图片和文件消息，且不推进全会话 reader cursor；
- MCP 层必须薄，SQL/解析/授权进入独立 service 层；
- 保留 provenance 与许可证说明。

完成后按 spec 的 Handoff 模板报告，并停在 M1 gate。
```

---

## 26. 当前默认决策

为避免 Codex 因开放问题停摆，本 spec 采用以下默认值：

- repo：`sightglass`；
- Python 3.11+；
- SQLite read model；
- FastMCP 或当前 WGO 已验证的 MCP Python stack；
- macOS-first；
- Unix domain socket 作为最终本地 IPC；
- stdio MCP 作为首个出口；
- native 初始化使用单会话 allowlist；经 owner 明确授权后，reader 可使用 all-except-denylist；
- Codex reader 只允许 synthetic fixtures；
- 群成员解析默认 conversation-scoped；canonical identity 只依赖 stable principal key；public handle 属于 mutable label/evidence；
- source path 只作为 installation locator，不定义账号 identity；
- timeline pagination 与 reader updates 分别使用 timeline sort key 和 observation sequence；
- reader timezone 显式保存，不依赖进程系统时区；
- 默认同时返回 reader-facing `label` 与可证明的 source `shown_as`，不伪造历史群名片；
- speaker-filtered reading 默认使用 participant/filter-set cursor，不推进 conversation cursor；
- 默认 strict validated reads：current semantic epoch 已 admission 时普通 message/inbox 使用带 bounded-stale receipt 的 materialized plane；search、cold miss、stale projection 与 discovery 继续要求 current-source validation；
- 默认无网络资源抓取；
- 默认不自动写入 OB；
- 默认 access logs 无正文；
- 新配置与新授权聊天默认 `on_demand`，既有 observed stock 保护到显式 scope preview/apply；附件 original 不自动长期归档；
- 默认 WGO adapters 直到 M5 才加入。

这些默认值可以在真实使用反馈后修订，contract 需要通过版本升级演化，不能静默改变现有语义。

---

## 27. 最重要的产品边界

这个产品的核心对象是 **用户想让授权 reader 查看、理解的微信现场**。

WGO 负责从高噪声群聊中筛选、沉淀和投影知识；本产品负责给授权 reader 一条最顺、最忠实、最少搬运的观看路径。

两者共享可靠的 source 技术与可追溯性，但不共享产品中心、权限状态和阅读 cursor。
