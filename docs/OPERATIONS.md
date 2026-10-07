# Operations — synthetic and native local installation

本 runbook covers deterministic synthetic runtime and the supported native macOS WeChat live source. Commands must run from the repository root. Never paste command output containing private source state into issues, commits, or public logs.

## Initialize once

从 repo root 执行：

```bash
uv sync --extra dev --extra macos-wechat
uv run sightglassctl synthetic-create /tmp/sightglass-synthetic
uv run sightglassctl init --source-root /tmp/sightglass-synthetic
uv run sightglassctl daemon start
uv run sightglassctl status
uv run sightglassctl doctor
```

默认 config、`window.db`、socket、spool、cache 和 daemon log 位于 `~/Library/Application Support/Sightglass/`。目录必须是 mode `0700`，普通 private files 与 socket 必须是 mode `0600`。reader/operator 原始 token 只存 macOS Keychain；config 与 `reader_profiles` 只存 hash。

`doctor.ready` 汇总 config/socket/DB permissions、Keychain availability 与 active source settings。`source_kind` / `source_mode` 独立标明当前 binding；`volume_encryption` 也是独立报告项。

## Activate native macOS WeChat

Preconditions: WeChat `4.1.13 / 269602 / arm64` 正常运行，source 可读，并已有与当前数据库匹配的 verified-key map。切换 source 前 daemon 必须停止。

```bash
uv run sightglassctl source providers
uv run sightglassctl source discover --kind macos-wechat
uv run sightglassctl daemon stop
uv run sightglassctl source init-macos-wechat \
  --key-file /private/path/to/verified-key-map.json
uv run sightglassctl daemon start
uv run sightglassctl doctor
```

已有 native binding 升级 branch/source implementation 时，保持 daemon 停止并运行：

```bash
uv run sightglassctl source init-macos-wechat --reuse-installed-keys
```

`discover` 只输出 opaque candidate 和 app profile，不输出 source path。`init-macos-wechat` 对新 key file 使用 bounded `O_NOFOLLOW` read 并验证读前/读后 identity；逐库验证 page-1 后，才把完整 keys 写入 source-account-scoped Sightglass Keychain item。`--reuse-installed-keys` 不创建 key file，也不把 key 放入 argv/stdout，而是在进程内读取同一 binding 的 Keychain item并重新完成全库 page-1 verification。两条路径都会创建 source-ID-v2 专用的 private `window.db`，并把 reader policy 切为 selected allowlist：只允许初始化时最新、实际可读的一个会话。第一次初始化生成 private stable account binding；同一 candidate 重新初始化会保留既有 account key/ID 与 Keychain scope，并把 legacy source settings 补成 profile-bound v2。旧 row-location-ID `window.db` 不会被删除或混入新 identity namespace，仍留在 private data directory 供人工回退/清理。导入文件不会成为 runtime dependency，可以继续由 owner 私下保管。

当前 source 使用 schema v10。正常启动不会 bulk upgrade、压缩、VACUUM 或自动删除旧库存；
旧 schema 会在 DDL 前被拒绝。Schema v9→v10 使用 [显式离线流程](#selective-residency-and-offline-compact)，
同一 frozen input 同时生成 exact recovery 与 candidate。更早 schema 先用其已支持且经验证的
旧 release／recovery 流程升级到 v9，再进入本流程；历史 migration helper 只用于 fixture，
不是当前 production startup 路径。SQLite 必须达到 3.43 并通过实际 contentless-delete probe；
`detail=none` 保留，`columnsize=0` 与该 backend 不兼容。

Daemon readiness 默认等待 120 秒，可用 `SIGHTGLASS_DAEMON_START_TIMEOUT_SECONDS` 覆盖；
超时会终止子进程并指向 private log。这个窗口不替代离线转换，也不授权延长 reader deadline。

Native V2 image decoder key 与 DB key map 分开管理，并绑定当前 installation 的 native account identity：

```bash
uv run sightglassctl source image-key status
uv run sightglassctl daemon stop
uv run sightglassctl source image-key import \
  --key-file /private/path/to/image-v2-key.hex
# 或交互地把同样的 32 个 hexadecimal characters 只写入 stdin：
uv run sightglassctl source image-key import --stdin
uv run sightglassctl daemon start

# 不再使用时，daemon 停止后执行：
uv run sightglassctl source image-key remove
```

`status` 只报告 `account_bound` / `enrolled`，不输出 Keychain account、settings path 或 key identity。Key file 必须是 owner-held、regular、single-link、mode `0600`，读取经过 bounded `O_NOFOLLOW` 与 identity checks；`--stdin` 同样只接受 exactly 32 hexadecimal characters。禁止 `--key <literal>`，因为 argv 会进入 process inspection / shell history surface。`import` / `remove` 必须在 daemon 停止时执行；Key 只在 Sightglass Keychain 中持久化。Enrollment 不是 first-party extraction，operator 必须已有可信 key material。

正常使用时保持 `sightglassd` 与微信运行。Daemon 的 bounded source worker 先轮询 authorized conversations 的 live tail，再处理至多一个 resumable backfill batch；它是日常 live refresh owner。Native live-tail phase 与 backfill phase 各自保留原 20 秒总预算；`SOURCE_GENERATION_CHANGED` 或 exact `SERVICE_TIMEOUT(reason="operation_deadline")` 会在同一 phase 内进入 fresh attempt。Live-tail 的 conversation scope 依次为 5 / 2 / 1，stale-tail depth 为默认 / 20 / 10，incremental batch 为默认 / 20 / 1；backfill batch 为 50 / 20 / 1。Stop、foreground cancellation、其他 source error 或最终 one-message attempt 仍失败时不会继续重试或猜测进度，durable position 留给下一轮。Backfill 直接复用 queue 时已 admission 且仍受当前 policy 检查的 source target，不在每批历史消息前重扫完整 catalog。大 catalog 通过 persisted round-robin cursor 跨轮覆盖，同一 generation 已完整 index 时 no-op，catalog activity 新于已 admission tail 时优先。Current-epoch 的普通 message pages 与 native inbox 从 `window.db` 物化读取，warm resource binding 从 CAS 读取；这些 local-only call 不会 foreground-cancel 或等待 source worker。Cold miss、stale semantic epoch、search/current-source validation、discovery 与首次 resource acquisition 仍进入 source-required lane。

Reader 需要补齐 partial context 或明确确认 source 时，可无 cursor 请求 `wechat_read_messages(refresh=true)`，它不启用全历史 backfill；source/space failure 保留原 strict error，默认读取仍可打开有效缓存。已 admission 的 message/context 局部页不依赖后台 tail completion；其 `has_more` 只描述本地已观察集合，coverage 会标出历史缺口。Native 首次无 cursor 的 recent source page 在同一验证提交中记录真实 tail，之后可本地复读；历史 anchor 不会使全会话 repair 提前完成。Cold native inbox 的已知 readiness error 直接返回，不先中断负责修复的后台 worker。Voice prepare 空间不足只降低 sidecar，不丢弃已读取文字；recent/range/speaker 的短进度提交可用 maintenance reserve，低于真实 free floor 时仍必须失败。

`wechat_status(summary, response_profile="diagnostic")` 使用 cached reader health，不打开 source，也不进入 tool slots/read gate；runtime retrieval 附加状态仍只读小型 published derivative metadata，不统计历史行数。返回的 `read_plane` 与 `readiness.indexed_reads/live_refresh/resource_cache/resource_acquisition/voice` 区分本地可读性与 live refresh，`runtime.operations`、`runtime.window_writer`、`runtime.receipt_writer` 与 `runtime.source_worker` 则区分 active work、writer wait、receipt backlog、foreground yield 和 source-worker/backfill state。Receipt queue 固定容量为 4096；`failed_count` 或 `dropped_count` 非零表示审计持久化发生故障或 saturation，需要先查本机 writer/DB 状态，但 reader success 不会因此被改写。已 admission 的 known native conversation 普通读取不打开 encrypted source snapshot；response receipt 明确 `served_from=window_db`、`freshness.state=bounded_stale` 与 `live_refresh_confirmed=false`。需要新鲜、完整 source catalog 时调用 `wechat_find_conversations`；search 仍对 candidate 做 current-source validation。`wechat_read_inbox` 使用 latest complete catalog epoch 中已达到当前 provider/parser projection epoch 的 observation snapshot，不要求 warm call 先完成 live health check，并在 coverage/source receipt 中报告 `catalog_fresh_as_of`、watermark、indexed/degraded conversation count 与 bounded-stale freshness；repair 未完成时返回 `source_projection_refresh_pending`，不混用旧 sender projection。普通 tool operation 的 cooperative deadline 是 25 秒；超时返回 retryable `SERVICE_TIMEOUT` 并释放 operation slot，相同 tool name/arguments 的并发请求 single-flight。普通 append 不进入已签发 materialized message/inbox traversal；当时已存在的 row 后续发生 correction、policy revision 或 semantic epoch 变化则 cursor stale。Main DB replacement、key/profile/database-set 变化仍需重新 reconcile 或 import。

连续 timeout 时可通过 authenticated operator IPC 单独读取 operation registry，避免完整 `sightglassctl status` 的 resource/cache aggregates。下面的 `daemon.status` 选项不进入 source/window DB、work lanes 或 reader gate；仅 operator token 可用，不属于 MCP tool：

```python
from sightglass.runtime.ipc import IPCClient

operations = IPCClient(role="operator").call(
    "daemon.status", {"operations_only": True, "include_stack": True}
)
print(operations)
```

`include_stack=true` 只采样最早开始的 active operation owner，返回最多八个 `sightglass.*` module/function/line；`active_phase` 是最接近当前执行位置的 public code location。它不返回 locals、arguments、SQL、file paths、thread IDs 或消息内容，也不保留 frame。省略该选项只返回 counters；idle 返回空 stack，普通 MCP/status 不采样。采样是当时的位置，需结合 operation age 与重复位置判断卡点；一次 aggregate writer-wait 数值不能单独归因某次 timeout。

当前 native runtime 应报告 `source.mode=live`、`source.implementation=sightglass.macos-wechat.sqlcipher.v6`、`synthetic_only=false`、`window_db_schema_version=10` 与 `resources=true`。Appmsg file/image 与已观测 digest-named `msg/video` payload 可进入 image/PDF/text/rich-file pipeline；resource finder 只查已物化 local catalog，HEIC/TIFF/BMP 支持本地 sniff／preview，UTF-16/GB18030 严格解码，audio/video 可返回有界 metadata，支持的本地视频可生成 PNG 首帧 preview。Thumbnail-only image 以 `preview_only` 提供 preview 而不冒充 original；encrypted V2 mapped original 在 decoder key 未 enrollment 时明确返回 `image_decoder_key_missing`，但若同一 message-position 另有可解码 cache thumbnail，则只以 `preview_only` 提供该 preview，绝不把 ciphertext 或 preview 冒充 original。当 `message/media_*.db` key 已 page-1 verified/enrolled 时，voice 可作为 `audio/silk` original 读取且 generic `metadata` 返回 `audio={format:"silk",mime_type:"audio/silk"}`；本地转写另需已安装 helper、decoder 和对应语言 speech assets。每次安装的客户端 playback/consumption 须单独验收。Sticker 由运行时 in-memory 推导的 account FileXorKey 本地解密，original 优先、message-bound thumbnail 降级，entry 缺失或推导失败保持显式 missing/key_missing/metadata_only。所有资源处理都没有 attachment network fallback。Catalog、roster、indexed history 和 local payload availability 都按实际 coverage 报告，不从 bounded empty result 推断全局不存在。

Native provider v6 更新 context identity admission：升级后的旧 v5 materialized projection/cursor 不再作为 current-epoch evidence；正常有界 source 重观察／tail repair 恢复可读性，不在启动时批量重写或 refill history。Pending immutable update deliveries 保留自己的 replay bytes。

遇到 source-required call 的 `SOURCE_INCOMPLETE` 或 `source_build_binding_changed` 时，不要绕过 profile/key verification。常见原因是微信升级、key rotation 或新 message shard；停止 daemon，用新版本支持与完整 verified-key map 重新执行初始化。修复前，current-epoch materialized pages、native inbox 与通过当前 policy/resolver 检查的 warm CAS object 可以继续读取，但 receipt 会明确 bounded-stale/local-cache，且不能用于推断 live source 当前状态。Sightglass 不会重签、注入或修改 WeChat。

## Lifecycle and policy

Reader 的显示时区保存在 config 的 `reader.timezone`，使用 IANA 名称。`uv run sightglassctl timezone status` 只读配置；修改时执行 `daemon stop` → `timezone set Asia/Shanghai` → `daemon start`。无效 timezone 或尚存 daemon socket 会拒绝修改；不需要重导入 source/key，也不改变 policy。旧 config 缺字段时兼容 `Asia/Singapore`，保存后显式持久化。这个设置只改变新消息页/search 的显示，UTC instant、排序、cursor、source 附件月份目录及未 ACK 的 immutable delivery 不受影响。

`runtime.source_worker.backfill_error_reason` 与 `backfill_error_elapsed_ms` 分别记录最近失败 backfill phase 的 reason 和耗时；`SERVICE_TIMEOUT / operation_deadline` 表示 backfill 自己用尽预算。前台抢占只增加 `foreground_yield_count`，保留已有 backfill 状态，不把 cancellation 写成新的 backfill error。一次成功 backfill phase 会清空这两个错误字段；旧错误在被前台频繁打断时仍可能可见。

```bash
uv run sightglassctl status
uv run sightglassctl pause
uv run sightglassctl resume
uv run sightglassctl daemon restart
uv run sightglassctl daemon stop

uv run sightglassctl scope status
uv run sightglassctl scope catalog --limit 100
uv run sightglassctl scope set selected
uv run sightglassctl scope set account --confirm-account-scope
uv run sightglassctl scope allow wxconv_...
uv run sightglassctl scope deny wxconv_...
uv run sightglassctl scope clear-deny wxconv_...

uv run sightglassctl conversation deny wxconv_...
uv run sightglassctl conversation allow wxconv_...
```

`scope set account` 的确认 flag 只证明 operator 明确执行该高范围切换；它不代替账号 owner 的实际授权。Account mode 映射为 `all_except_denylist`，新发现 conversation 自动受当前 denylist 约束。`scope set selected` 回到 allowlist，至少需要一个 allowed conversation。Policy 变化会使受影响的旧 pending delivery 失效并使 policy-bound catalog/inbox cursors stale，防止旧 payload/遍历跨 policy replay。Pause 后除 `wechat_status` 外的 MCP reads 返回 `SERVICE_PAUSED`；resume 只能从 operator CLI 执行。

## Tail and historical backfill

```bash
uv run sightglassctl backfill status
uv run sightglassctl backfill conversation wxconv_... --max-messages 10000
uv run sightglassctl backfill account --max-messages 10000
uv run sightglassctl backfill account --from 2025-01-01T00:00:00Z --before 2026-01-01T00:00:00Z --max-messages 100000
uv run sightglassctl backfill pause
uv run sightglassctl backfill resume
```

Account backfill queues one bounded durable job per policy-authorized keep conversation；`--max-messages` 是每个 conversation 的明确上限，不是“无限历史”。Worker 始终先做 live tail，再做至多一个 backfill batch；restart 从 durable resume cursor 继续。一个 live batch 遇到 WAL drift 或 exact operation-deadline timeout，会在同一 20 秒总预算内用 fresh 50 / 20 / 1 batch attempts；最后一条消息也无法在其 attempt 内读取时保留原 processed position，下一轮继续，不要求关闭微信。`backfill status` 只应报告 job/count/state/error code，不输出标题、正文、filename、source path 或 cursor body。Search receipt 在 jobs 完成前可以合法报告 `history_not_fully_indexed`；这不是空搜索结果的全局无匹配证明。

Resource／voice／derived workers 现在先读 durable 状态，只有实际到期或可 claim 的工作
才取得短 writer transaction；空 resource lease recovery 不取得 writer。每种 worker 有自己的
Event，新任务、配置／rebuild 通知与最近 retry／lease deadline 驱动等待，默认最长 30 秒
兜底。Voice 与 resource 新任务通知在最外层 commit、writer 释放后触发，rollback 丢弃通知；
Event 只是提示，durable queue 与 fencing 仍是 authority。Settled 的纯 on-demand scope
降低 metadata polling 到 30 秒，keep／recent 与 incomplete catalog 保持既有 2 秒节奏；
foreground／operator 修改会唤醒 source worker，不自动恢复已释放历史。

Worker status 将当前 `last_error_code/type/location` 与有界最后历史错误
`historical_error_count/code/type/location` 分开；确认成功或 idle 后当前错误清空，历史仍保留。
Location 只有公开 module/function/line，绝不包含异常消息、args、notes、traceback 路径或
frame 数据。`idle_cycle_count`、`work_count`、`wake_count` 是进程内计数；read-plane ready
不证明正文、links、lexical、resource 或历史 coverage 全部收敛。`sightglassctl status` 的
`source_connections` 为 native aggregate handle counts（synthetic 为 null）；idle 最多 16、
age 阈值 60 秒，active/waiting 和 scoped handles 不受 idle cap 逐出。

普通正文的新 admission 只写 `messages.text`；`structured_json` 保存结构，`search_text`
只保留与正文不同的 card/search document。旧 v10 行继续通过同一 current-body helper 读取，
正常 startup 不做全库 rewrite。显式 stopped-only compact 会移除完全相同的旧当前副本，
verifier 比较这项声明过的表示变换，frozen recovery 仍逐字节恢复原输入；嵌入正文与
body column 冲突时 fail closed。`--copy-current` 仍是 exact byte-copy relocation，
不调用该 normalization。已释放正文不会从 observation 重建；pending spool 或 active job
仍依照现有 protection 保留其输入，release 不表示 secure erase 或备份清除。
Compact 的断点与完成 receipt 绑定 `sightglass.current-body.v1` 转换；若旧 candidate 没有
该版本或转换版本不同，保留其 recovery／旧 runtime，使用新的 candidate 目标路径。
不能用新转换接续旧 candidate 的已复制 chunks；原格式仍可由原版本 runtime 验证和恢复。

## Storage budget and maintenance

`sightglassctl storage status` 返回 content-free 分项字节数、reserved/available/remaining bytes、limits 和 `ok` / `soft_limit` / `hard_limit`。Daemon 已支持预算时，operator status 会完整 reconcile 私有文件，并补充 message count、DB bytes/message 和 freelist bytes；离线或仍运行旧 daemon 时，新 CLI 只盘点文件，不打开或迁移数据库。新 CLI/旧 daemon 的过渡期 fallback 仅限缺少 storage status 的响应；安装切换完成后使用 daemon 的统一统计。MCP summary 只更新可变文件 stats 与 filesystem free bytes，不扫描所有对象、不查询消息表。

| Config `storage` 字段 | 默认 | 作用 |
| --- | --- | --- |
| `soft_limit_bytes` | 4294967296（4 GiB） | 暂停 queued/running backfill，停止新增 voice batch、下一 step 和 worker lease；普通读取/tail 可继续到 hard boundary |
| `hard_limit_bytes` | 6442450944（6 GiB） | 拒绝 hydrate/tail/new delivery/resource growth；`wechat_status.ready=false` |
| `min_free_bytes` | 2147483648（2 GiB） | 所有受管写入必须保留的 filesystem free floor |
| `maintenance_reserve_bytes` | 268435456（256 MiB） | 普通写入额外预留；仅已 admission 页的 reader-position、ACK、pause、受控 cleanup 与必要 voice recovery 等短事务可使用 |

省略 `storage` 的既有 config v2 使用这些默认值；不是 unlimited mode。`0 < soft < hard`，reserve 至少 1 MiB。所有值以整数 bytes 保存。配置改变须先停止 daemon；命令本身不会启动 daemon 或恢复 backfill：

```bash
uv run sightglassctl storage status
uv run sightglassctl storage explain
# 在已授权的维护窗口中，先停止 daemon，再调整所需字段。
uv run sightglassctl storage configure --soft-limit-bytes 4294967296 --hard-limit-bytes 6442450944 --min-free-bytes 2147483648 --maintenance-reserve-bytes 268435456
```

显式 recovery 可使用 stopped-only backup lifecycle；schema v10 转换另走下文 compact／pair 流程。所有命令先确认 daemon 已停止并取得同一个 process lock；artifact 与 manifest 只能位于 `window.db` 同目录，均为 mode `0600`。`plan` 会对每个 compressed snapshot 重新计算 compressed digest，并完整解压计算 raw digest；大库上可能需要一段时间，但不修改任何文件：

```bash
# 先把已运行的 installation 持久化为 paused，再停止；新 daemon 会保持 paused。
uv run sightglassctl pause
uv run sightglassctl daemon stop

# 检查 current DB identity、传统 raw backups、compressed snapshots 和 exact acks。
uv run sightglassctl storage backup plan

# 如空间不足且 plan 精确列出了可退休的旧 raw migration backups，人工核对 targets 后：
uv run sightglassctl storage backup retire --ack '<legacy_retire_ack>'

# 为当前 schema/current bytes 创建（或复用）一个相邻 zstd level-1 snapshot。
uv run sightglassctl storage backup create
uv run sightglassctl storage backup plan

# 仅重启与当前 schema 兼容的 runtime；backup create 本身不转换 schema。
uv run sightglassctl daemon start
uv run sightglassctl status
uv run sightglassctl doctor
uv run sightglassctl resume
```

`retire` 和 `restore` 都是破坏性动作，必须使用同一次 current `plan` 给出的 exact acknowledgement；目标 identity、mtime、inode、size、manifest 或 digest 变化都会使 ack 失效。默认 `retire --ack` 只删除 plan 中列出的传统 raw backups；compressed snapshot 必须同时给出 exact filename。`restore` 会验证 snapshot 后在同目录生成并 fsync 一个 private temporary DB，执行 SQLite `quick_check`／schema check，以 durable journal 把原 DB 和 WAL/SHM/journal 移入相邻 private rollback namespace，再 atomic replace `window.db`。New DB 与目录 fsync 完成后才记录 committed，并回收旧 namespace；在此之前崩溃会恢复原 DB 与 sidecars，之后崩溃会保留新 DB。`WindowDB` open/create 与 stopped-only CLI backup 入口先执行同一 recovery hook；未知文件、symlink 或不私有的 namespace fail closed，绝不猜测或删除：

```bash
uv run sightglassctl daemon stop
uv run sightglassctl storage backup plan
uv run sightglassctl storage backup restore \
  --artifact window.db.v5.backup.zst --ack '<restore_ack>'

# 只有完成迁移后的 runtime/stdio/tunnel acceptance，且确认不再需要该 rollback point 后才退休：
uv run sightglassctl storage backup plan
uv run sightglassctl storage backup retire \
  --artifact window.db.v5.backup.zst --ack '<compressed_retire_ack>'
```

Restore 只恢复 `window.db` bytes，不切换 executable、branch 或 tunnel child；若目的是回退 schema/binary，必须在重新启动前同时选用能读取该 schema 的已知 binary。不要把 snapshot 移到未加密卷、把 artifact 名当作公开信息，或在 daemon/live SQLite handle 尚存时手工复制、删除这些文件。

`storage explain` 是 read-only operator 诊断面，默认返回 **quick** 容量概况：daemon 已追踪的 owned-byte 分项、实时 filesystem floor／reservation、SQLite page/freelist bytes、分页 tracked regular files，以及既有 7/30 天 history。默认路径不扫描 message/observation 正文、不逐表 COUNT，也不递归重扫所有 CAS 文件。`headroom` 报告一次基础 SQL admission 所需 minimum available bytes、filesystem/budget shortfall；实际大 payload 仍需额外 reservation。`growth` 区分 operator pause 与 storage 所允许的 foreground/background 增长，不降低预算或自动清理。默认 `--limit 500 --offset 0`，单页上限 2,000，`next_offset` 可继续；tracked inventory 表示 runtime 账本，不把未做 reconciliation 的全文件枚举宣称为即时完整扫描。

精确统计须显式请求 `storage explain --deep`。`--phase all|tables|layout|observations|sample` 可以分别请求 table counts、dbstat physical layout、TEXT/BLOB counts 或 bounded compression sample；默认 deadline 10 秒，`--deadline-seconds` 可设为大于 0、至多 25 秒。预算从 operator gate 等待前开始，SQLite progress handler 取消正在运行的 SQL；IPC 客户端断连或 daemon shutdown 同样取消。返回 `diagnostic.state=partial`、`reason=operation_deadline|operation_cancelled` 与已完成阶段／table，不把 partial 当成完整容量 breakdown。`tables` 阶段可使用 `--after-object <last_completed_object>` 继续，显式 phase 可独立重新请求；大表精确 COUNT 仍可能在预算内无法完成。每个统计 statement 使用独立短读视图，`database.snapshot_scope=one_statement_per_phase_result` 明确它们不是同一时刻的全库 snapshot。

```bash
sightglassctl storage explain
sightglassctl storage explain --deep --phase layout --deadline-seconds 10
sightglassctl storage explain --deep --phase tables --after-object messages
sightglassctl storage explain --deep --phase sample --sample-size 128
```

文件输出只有 root-relative path、category、logical/allocated/accounted bytes、mtime、recognized role 与保守的 `safe_action`，不读取内容、不跟随 symlink、不返回 configured absolute roots。相对 filename 仍是 private operator metadata，输出不应直接分享。Compression sample 至多检查 1,000 条最早 observation，编码其中的 legacy TEXT，不为凑足 legacy 样本扫过任意长度的 BLOB 前缀；返回 examined/sample rows 和 byte counts，不返回原文。只有本次 observations 阶段取得精确 legacy count 时才给全量 payload estimate。所有阶段均不删除、迁移、VACUUM 或写入 history，保持 `mutated=false`。

Daemon 在启动／重启时记录当天 snapshot，运行中跨过 UTC 日期边界时再记录；同一天的新 snapshot 替换旧值，不累积重复条目。`storage-history.json` 是独立于 `window.db` 的严格 versioned JSON sidecar，最多保留 64 个 UTC 日，只包含分项 accounted bytes、message/observation counts 与 UTC timestamp，不含消息正文、label、ID、filename、relative/absolute source path 或 credential。它以 mode `0600`、single-link、no-follow 读取和同目录 atomic replace 保存，并计入统一 owned-storage budget；写入失败只降低 `daemon.status.storage_history` 的 content-free availability，不阻断普通 reader 功能。

`storage explain.history.windows.7d` 与 `.30d` 只在 `as_of_day` 和精确的第 7／30 天 snapshot 同时存在时可用，不会以更短区间近似。可用窗口报告 accounted/database/other-owned/resource bytes、message/observation count 差值、平均每日 accounted bytes、每新增 message bytes，以及正增长时到当前 soft/hard limit 的线性估算；估算不是配额保证。`storage explain` 只读取已经保存的 sidecar，仍保持 `mutated=false`。

Daemon 未运行时，`storage explain` 只做 file inventory、读取既有 history sidecar，并明确返回 `database.available=false`；它不会为了诊断打开、初始化或迁移 `window.db`，也不会生成当天 snapshot。Daemon 运行但版本过旧、不支持 `operator.storage.explain` 时，命令明确失败，不绕过 daemon 打开 live DB。quick mode 不收集 counts，count 字段保持 `null`；deep 的 `dbstat` 不可用时 physical layout unavailable；legacy sample 的 `estimated_payload_reduction_bytes` 只估算 payload encoding 差异，不是文件系统可立即回收字节。首次启动只有一天数据，7/30 天窗口会明确报告 exact baseline 不存在；需要相应 UTC 日跨度后才可用。

预算覆盖配置的 data directory 与 window DB 所在目录的去重并集，包含 DB、WAL/SHM/journal、单列的 migration backups、delivery spool、resource objects/tmp、voice/processor staging、日志和留在这些目录中的其他文件。每个 regular file 按 logical size 与 filesystem allocation 的较大值记账；不跟随 symlink。目录须专供 Sightglass，外部 WeChat/source 存储不属于预算。不可变文件在写入/删除点增量记账，启动和 operator status 全盘 reconcile。Processor 临时文件留在受管私有卷；并发任务先 reservation，SQLite 写事务在 BEGIN 前检查、prepared batch 写入前增加估算、commit 前复核。

预算提供 **admission backpressure，不是 OS quota**：SQLite page/WAL amplification 是估算，外部进程也可能消耗磁盘。大 batch、文件系统行为或其他写入仍可能使实测值越过阈值；reserve 是可耗尽的有限空间，不保证物理磁盘满时 ACK 一定成功。`estimated_growth_bytes` 使用每条 16 KiB 的 planning estimate，不是实际大小或严格上界；压缩率取决于消息分布。Backfill 达软阈值会保留 processed position 并 durable pause，空间恢复后仍需 operator 显式 `backfill resume`。Voice pending job 保留，容量恢复后 worker 可以继续；capture/commit 中途受阻会用 reserve 归还 lease 并保存小状态事件，限速重试且不消耗识别失败次数，不把 storage pressure 标成永久转写失败。

容量不足返回 retryable `STORAGE_PRESSURE`，不会自动删历史或推进未交付 cursor。当前 policy 仍允许的 pending response 可直接从 immutable spool exact replay。若请求带有效 `ack_delivery_id`，新页因容量受阻时允许 ACK 单独使用 maintenance reserve 提交，错误 details 带 `ack_committed=true` 和同一 delivery ID；客户端不应再把该 delivery 当作未确认。普通 source failure 仍整体 rollback ACK。拒绝、过期或跨 scope 的 ACK 不因容量不足获得权限。异步 access receipts 也受写预算限制；写入受阻会计入既有 receipt-writer failure diagnostics，不阻塞已经成功的 replay。

新 `message_observations.parsed_json` 值是 `SGOC` version-1 BLOB：header 保存 codec、原始 UTF-8 长度和 CRC32，body 为 zlib 或 raw bytes，payload digest 仍对原始 bytes 计算。历史 TEXT 行按原样读取；column name/affinity 保留兼容，observation codec 与 SQL schema 版本独立。Schema v10 转换仅在 explicit stopped-only candidate 中执行，正常启动不转换旧库。Identity correction 通过唯一 codec 解码，坏 stream/长度/CRC/JSON 会 fail closed。Updates 通过 observation sequence 选择当前 message projection，pending exact replay 依赖独立 spool。相同 active label/provenance 的重观察只更新 freshness；A→B→A 保留三个真实区间，message-surface label 仍按消息保存。


Schema v9 不把旧 `complete`／min-max 范围直接转换为连续覆盖证据。旧 rows 保留，但连续 frontier、历史完整性与 source-read windows 需要 bounded source revalidation；前台 recent 可以保存离散已验证窗口，后台仍从连续 frontier 追赶。窗口之外的本地邻居不能冒充连续前后文。这条 v9 覆盖合同在 v10 保留；升级前须 exact current recovery point，source readiness 与实际安装 schema 分别记录。

存量 observation 一致性通过 operator-only 命令检查与修复：

```bash
sightglassctl maintenance observations inspect --limit 100
sightglassctl maintenance observations inspect --limit 100 --after-message-id wxmsg_FROM_CHECKPOINT
sightglassctl maintenance observations repair --limit 100
```

`inspect` 只读，一次最多 500 条，返回 content-free counts 与 continuation；`repair` 必须显式调用，一次最多 500 条，使用 DB 内 durable checkpoint，重启后再次运行即可续批。修复不打开 source、不改写历史 observation，也不以旧 payload 覆盖合法 identity correction；它追加与 current projection 匹配的 observation episode，或令无法证实的 projection 等待正常 source 重观察，同时使受影响 cursor／derived version 失效。Repair 仍受原 storage admission/floor 约束，失败 batch 不推进 checkpoint。输出中的 checkpoint ID 属于 private operator metadata，不应提交或分享。Exact pending replay 继续由 immutable spool 保持。

普通运行只对新 observation 使用当前编码，**不会在 daemon startup 做全库转换、VACUUM 或新建一份数据库**。不支持当前 schema／codec 的旧 binary 不能直接打开新 DB。全库转换／compaction 由下节 stopped-only `storage compact` commands 提供；真实操作须另行授权，并准备私有加密卷上的空间、停写与完整 runtime／DB／state recovery pair。不得把 SQLite freelist 当作全部内部空隙，也不能以 freelist 很少推断 VACUUM 最大收益；活动 WAL 上不使用 `immutable=1` 来规避锁。`window.db` 包含 reader state/corrections/bindings，删库不是无损清理。先用 cache dry-run 确认真正可清对象，再决定释放空间或调整预算；任何 live 清理、迁移、restart、resume 都属于独立 operator action。

可复现的物理测量只使用生成的 synthetic 数据，并在结束时删除自身临时 stores；`--keep` 可保留人工检查。比较库内容一致，两边均 VACUUM 后测量，结果表示紧凑文件潜在差异，不等于对既有 live DB 执行在线更新能立即收回的空间：

```bash
uv run --no-sync python scripts/benchmark-storage.py --messages 10000 --json
uv run --no-sync python scripts/benchmark-storage.py --messages 1000000 --repeats 1 --json
```

Benchmark 报告 DB/table/index physical bytes、bytes/message、payload bytes 与扫描解码/identity-correction 耗时。构造阶段使用同一 synthetic transaction 与 64 MiB SQLite page cache；不测或声称生产 ingest throughput。扫描计时包含 SQLite I/O/JSON/codec，单次结果受缓存和机器负载影响。分布含短英文、中文、长文、image metadata、link 和 forwarded records；重复 key 词汇偏多，应与授权本机抽样分开解释，不能推广为所有账号的保证。`--out` 只指定 synthetic 工作目录，脚本从不接收已有数据库路径。

## Selective residency and offline compact

Source schema v10 把 access 和 retention 分开。ReaderPolicy 仍决定可读范围；排除访问不构成第四种
retention mode。新配置／新会话默认 `on_demand`。`keep` 持续收未来消息；`--keep-backfill`
只允许已选历史 jobs，创建 job 另用 `backfill conversation`。`recent` 默认 30 天／每会话 512 MiB；按需缓存默认 24 小时／
每会话 256 MiB；recent 与 on-demand 共用 1 GiB temporary-body cap。外层 owned-storage budget
照常覆盖 DB、WAL、附件与所有 sidecars。旧库存默认 protected；改 mode 不自动释放它。

以下是 operator IPC，daemon 必须运行；它们不是 MCP tools。`list` 分页覆盖已 admission 的完整
会话 catalog，按实际 tracked body bytes 降序或 conversation ID 排序。输出有 override／effective
mode、原因、resident count／bytes 和 local time span；span 只证明部分驻留，不能推断完整历史。Native inbox 同样只显示有效 resident observations，
`coverage.message_scope="resident"`；未常驻会话不阻塞 catalog readiness，也不会伪造 tail completion。
旧未转换的 untracked bytes 明确 unknown，不以零冒充；v10 offline conversion 会计量并保护旧 stock。
Batch `set` 最多 500 个 IDs，全部 scope 校验成功才提交。

```bash
sightglassctl residency status
sightglassctl residency list --sort bytes --limit 200
sightglassctl residency list --sort bytes --limit 200 --cursor '<next_cursor>'
sightglassctl residency set keep wxconv_SELECTED
sightglassctl residency set keep wxconv_SELECTED --keep-backfill
sightglassctl backfill conversation wxconv_SELECTED --max-messages 10000
sightglassctl residency set recent wxconv_SELECTED --recent-window-days 30 --recent-max-bytes 536870912
sightglassctl residency set on_demand wxconv_SELECTED wxconv_ANOTHER
sightglassctl residency configure --lease-ttl-seconds 86400 --lease-max-bytes 268435456 --global-max-bytes 1073741824
```

On-demand idle sync 不打开正文，也不为 expired history 安排 refill。前台 range／context／refresh
只 admission 该有界页；搜索的 source scan 与 resident admission 分开，只保留 literal／speaker
匹配的候选，不把扫过的非命中正文全部缓存。需要邻居时再做显式有界 context read。
到期／超 cap 会释放 projection text/search/structured、历史 observation body copies 与本地派生
links/FTS；只保留 versioned identity header、原 payload digest、episode、actor／alias／correction、
observed progress 和 exact active dependencies。TTL 到期的正文不再参与普通 local result，即使
物理批次尚未结束。Pending spool 按原 bytes replay；resource／voice jobs 仅 pin 关联消息。
相同 episode 重读恢复其正文，不伪造 update。Cursor 因 residency revision 改变而 stale；
`residency rebaseline wxconv_SELECTED` 显式作废该会话旧 delivery／cursor scope，保留 observed progress。

释放旧库存需要先看 exact plan，再由 owner 批准 apply。Plan 绑定会话、message IDs、current
episode、body bytes、pins 和 residency revision；发生变化须重新 preview。单页最多 500 messages，
每次最多处理 500 observation copies，返回 `pending_messages`／continuation。已经批准的队列可
跨重启继续；不要对 stale plan 重复 apply。读 lease 不是把全会话 pin 住的永久租约。

```bash
umask 077
sightglassctl residency preview wxconv_SELECTED --limit 200 > "$PRIVATE_RELEASE_PLAN"
# Owner 已批准该 exact scope 后：
sightglassctl residency release wxconv_SELECTED --apply --plan-file "$PRIVATE_RELEASE_PLAN"
sightglassctl cache cleanup
sightglassctl cache cleanup --apply
```

Cleanup dry-run 不释放正文；apply／正常 bounded expiry 会继续已批准的 unfinished release jobs。
Payload bytes 的释放量不等于 filesystem reclaim；SQLite 会留下可重用 pages，物理缩小须另行 compact。

Offline conversion 接受 schema 9 或 10，必须在 daemon 停止并持有同一 process lock 时进行。
默认 preview 只读 metadata，不开 WindowDB、不迁移或 whole-history COUNT。带 conversation ID 的
preview 最多 500 messages／页，保留 `next_cursor`，其 private plan 还绑定 main/WAL identity。
Owner 要清全部旧正文时，可显式 `storage compact preview --all-stock`。这个 deep preview
逐条检查有效 dependencies，只输出 counts 和绑定整个 committed input 的小 plan，不把百万条
message IDs 装入内存。把返回的 `.plan` 放入 `--release-plans-file` 的单元素数组；不能与分页
plans 混用。冻结前源文件 revision 变化会拒绝该 plan。候选和验证均从同一 frozen input
流式决定 membership，pending replay 和 active resource／voice inputs 继续保留。
全局 FTS/layout/count 扫描仍使用明确的 deep diagnostics。Native workspace 与 pair root 必须在
实际验证加密的本地卷上；mode 0700/0600 不等于加密。以下变量必须由 operator 设为 private
绝对路径；`CANDIDATE_RELEASE`／`ORIGINAL_RELEASE` 指含 `bin/` 的已验证 venv root。
`WORKSPACE_BUDGET_BYTES` 须根据完整 peak 评估，而不是把 maintenance reserve 当迁移许可。

```bash
# Preview 可对尚未转换的 DB 执行，保持 read-only。
"$CANDIDATE_RELEASE/bin/sightglassctl" --config "$ORIGINAL_CONFIG" storage compact preview
"$CANDIDATE_RELEASE/bin/sightglassctl" --config "$ORIGINAL_CONFIG" storage compact preview \
  --conversation-id wxconv_SELECTED --limit 200

# Owner 已授权 exact candidate construction / 停机窗口后：
sightglassctl pause
sightglassctl daemon stop
"$CANDIDATE_RELEASE/bin/sightglassctl" --config "$ORIGINAL_CONFIG" storage compact build \
  --workspace "$COMPACT_WORKSPACE" --workspace-budget-bytes "$WORKSPACE_BUDGET_BYTES"
"$CANDIDATE_RELEASE/bin/sightglassctl" --config "$ORIGINAL_CONFIG" storage compact verify \
  --workspace "$COMPACT_WORKSPACE"
```

若 owner 已批准释放指定旧副本，可给 `build` 加 `--release-plans-file "$APPROVED_PLANS"`；该 private
JSON 必须是 preview 中 `.plan` objects 的数组，分页结果不能变成更宽 scope。未指定时完整保留
旧 body stock，仅转换 codec/backend。冻结和 recovery 使用同一 committed input：`frozen.db`、
exact compressed recovery／manifest、`candidate.db`／manifest 都在私有 workspace。转换在每批
copy 中做 SGOC／approved release，然后重建 contentless-delete FTS；没有先整体 rewrite 原 DB。
Checkpoint 在 committed batch 后发布；中断后用相同 workspace／scope 继续。完整 verification
比较 rowid→identity、sqlite_sequence high water、durable rows 与原始 observation bytes／retained
headers，检查 FK／SQLite consistency。恢复旧 namespace 不需要删除 active DB 来腾空间。

实际 peak 包括 frozen input、recovery、candidate、WAL/temp、同卷 staging 和独立 private state。
每批与每文件都检查 workspace budget 和 config physical free floor，已存在的不完整 artifacts
仍计入；旧 active DB／old release／recovery 均保持。开始实际 installation 前另外核对候选 namespace
的 owned bytes、运行 WAL peak、free floor 和可用 old runtime；synthetic receipt 不能替代真实容量计划。

`prepare-pair` 默认登记原 runtime／DB；带 `--workspace` 时验证并 copy candidate 到
operator 选择的 pair-root filesystem。源库可在另一卷；staging、完整新 namespace 和 atomic selector
都在目标卷，不跨卷 rename，也不提前移除源库。Native 目标卷仍必须验证加密，workspace budget 和
free floor 仍适用；目标卷必须在运行时保持挂载。它同时 clone immutable pending spool、referenced CAS、token-secret
和已存在的 semantic／search／storage-history sidecars。`helper_path` 为空时还独立复制默认
`<data_dir>/voice/sightglass-transcribe`，保留 owner-only 执行 mode；helper 必须 owner-private、
非空、single-link、regular 且可执行。复制前后与首次 activation 核验 source/target 的
identity、mode 和 digest；缺失 helper 保持缺失，staging 后新出现须重新 prepare。显式 external
helper path 保留原引用。所有复制字节都计入 workspace budget 和 physical free floor。
只有内部 storage paths 重定位，ID／digest
和 payload bytes 不变。新 config 的 data directory 选择独立 namespace，保留原 source binding、
policy 和 socket；默认 paused。新版本 ACK／cleanup 无法删除 old pair 的回退资源。Old namespace
占用仍影响物理 free floor，保持在新的 active owned budget 之外，不隐式删除它。

```bash
# 先用原 release 登记 original pair，保存返回的 OLD_PAIR_ID。
"$CANDIDATE_RELEASE/bin/sightglassctl" --config "$ORIGINAL_CONFIG" storage compact prepare-pair \
  --pair-root "$PAIR_ROOT" --runtime-python "$ORIGINAL_RELEASE/bin/python" \
  --workspace-budget-bytes "$WORKSPACE_BUDGET_BYTES"
"$CANDIDATE_RELEASE/bin/sightglassctl" --config "$ORIGINAL_CONFIG" storage compact activate-pair \
  --pair-root "$PAIR_ROOT" --pair-id "$OLD_PAIR_ID"

# 保存返回的 NEW_PAIR_ID；此步骤仍不更改 selection。
"$CANDIDATE_RELEASE/bin/sightglassctl" --config "$ORIGINAL_CONFIG" storage compact prepare-pair \
  --pair-root "$PAIR_ROOT" --runtime-python "$CANDIDATE_RELEASE/bin/python" \
  --workspace "$COMPACT_WORKSPACE" --workspace-budget-bytes "$WORKSPACE_BUDGET_BYTES"
# Owner 已批准 activation 后，单次 atomic selector 同时选择 runtime/config/DB/private state。
"$CANDIDATE_RELEASE/bin/sightglassctl" --config "$ORIGINAL_CONFIG" storage compact activate-pair \
  --pair-root "$PAIR_ROOT" --pair-id "$NEW_PAIR_ID" --expected-current "$OLD_PAIR_ID"
"$CANDIDATE_RELEASE/bin/sightglass-pair" --root "$PAIR_ROOT" ctl daemon start
"$CANDIDATE_RELEASE/bin/sightglass-pair" --root "$PAIR_ROOT" ctl status
"$CANDIDATE_RELEASE/bin/sightglass-pair" --root "$PAIR_ROOT" ctl doctor
```

已经处于当前 schema 的安装可用 `prepare-pair --copy-current` 搬到另一个
pair-root；它与 `--workspace` 互斥。先正常停止 daemon 并完成 WAL checkpoint，
然后以当前 config 调用：

```bash
"$CANDIDATE_RELEASE/bin/sightglassctl" --config "$CURRENT_CONFIG" storage compact prepare-pair \
  --pair-root "$DESTINATION_PAIR_ROOT" --runtime-python "$CANDIDATE_RELEASE/bin/python" \
  --copy-current --workspace-budget-bytes "$WORKSPACE_BUDGET_BYTES"
```

此路径对当前 DB 做逐字节核验的独立复制，复用相同 private-state clone（含默认 voice helper）、path relocation、
source revision fence 与 atomic selection，不重跑 codec／release／FTS 转换。未 checkpoint
的 WAL、staging 后改变的源库／目标库／私有状态都会阻止首次 activation。原 pair 保留作为
显式回退点；先前 conversion 的 exact recovery 仍须保留，搬迁本身不生成或退休它。
跨 pair-root 时先在目标 root 登记并选中原 pair，再让 stable wrappers 指向该 selector，
最后激活新 pair；三个命令必须始终使用同一 bootstrap。验收仍先 paused，再显式 resume。

固定 bootstrap 选择 `sightglass-pair --root ... ctl|daemon|mcp`，拒绝独立 `--config` override。
Stable command wrappers／原 tunnel command 必须都通过同一个 selector；旧 direct runtime links
不会自动切换。Runtime source/dependency provenance、schema/backend、candidate 与 recovery 均验证
后才 selection；activate 前 original DB 若有新写入则拒绝，须重新准备。Crash cut 由
`storage compact recover-pair --pair-root "$PAIR_ROOT"` 对齐 journal，选择只能是完整 old 或 new pair。
验收失败时先停止 selected daemon，再用 `activate-pair --pair-id "$OLD_PAIR_ID"
--expected-current "$NEW_PAIR_ID"` 选择整个 old pair，然后从同一 bootstrap 启动。恢复会回到冻结
时点，paused acceptance 期间决定 rollback；resume 后的新写入不由 old pair 保存。Semantic remote
rows 不回退／不删除；所有 remote candidates 仍受 current canonical admission 检查。Retirement
是另外的 owner 操作，本流程不删除任一 pair、old DB 或 recovery。

## Ordinary search preparation

普通 daemon search 首页返回 preparing；使用原参数和 `reading_token`（旧 host
可用 `cursor`）取结果，failed 时停止轮询，expired 时重开首调用。最终
`next_cursor` 是搜索分页，与 preparation token 不同。运行状态只包含有界进度，
不含 query text；后台完成仍须新的 current-source canonical validation。

`window.db` 同目录的 `search-preparation.json` 是 schema-independent、mode-0600
atomic 私有 job sidecar，最多 32 个 / 256 KiB / 15 分钟。它随既有 owned-file
accounting/reservation 管理，不需要新的 window migration。它只保存 request digest、
scope、状态和已 admission conversation checkpoint；不要提交、导出或手工改写。
Daemon restart 保留 job；同参数 token poll 重新在内存提供 query／speaker 条件后，重新扫描未完成的 conversation。Query／speaker text 不持久化；pause 会取消 work 并
令旧 job/token 失败，resume 不恢复该 token。Policy/source/store binding 变化使旧 token
失效。Selected source logical replacement 返回 `CURSOR_STALE`，需新首调用；ordinary
append 与 unrelated replacement 不改变该 token binding。一次 attempt 最多 120 秒，
每个 source quantum 最多 2 秒 / 1,024 positions；generation drift 最多三次，最终错误
明确返回，不能增加前台 25 秒 deadline 来掩盖失败。用户发起的 preparation 属于
ordinary admission，软压力不自动拒绝它；hard pressure 仍 fail closed，terminal/
cancellation metadata 可使用既有 maintenance allowance，不放宽 physical floor。
Terminal 状态仅在 atomic metadata commit 后发布；连此写入也失败时 token 保持
preparing/`state_commit_pending`，当前 worker 只重试 metadata，不复跑 source。

旧 wheel 不消费这个 sidecar；schema v10 admission 受统一 residency decision 约束。
重新提升到本版时，sidecar 内 token 仍受原 expiry/policy/source/store fences 约束。

## Retrieval indexes and operator commands

`find_links`/`retrieve` 的 link 与 lexical 表示都是可由 canonical messages 重建的 derivative。Derived-index worker 是 daemon sibling，按至多 100 的 batch 在自己的 lane 内补齐；它从不打开 source、不推进 reader state、不调度 voice/preview，容量压力下暂停重建但保留普通读取/replay/ACK。Warm result／pagination 沿用 materialized read；cold `find_links`／`retrieve` 通过既有 search-preparation worker 的 bounded physical-row pages 找候选，再 canonical lookup 核验命中，只缓存命中和有界跨页 context／reply。首调用的 `reading_token` 用于 poll；partial 结果中的 `source_continuation.reading_token` 才启动下一轮，重复提交同一个 continuation 不多推进。无 token 是 fresh query；`page.next_cursor` 只翻 materialized 结果。每轮有 row／body／conversation／time budget；native context 的无索引 position scan 受整体 deadline 与取消检查约束，不计作只有 1,000 个物理位置。完成遍历也不等于 atomic full-source snapshot。内部 checkpoint／generation 不投影给客户端；不 commit timeline position。普通 `read_messages` 的 link projection 仍脱敏；`find_links`/`retrieve` 按 owner 决定返回完整观察到的 URL，且任何 tool 都不会访问 URL。

Operator-only 诊断与重建面（不属于 MCP，需要 operator token）：

```bash
uv run sightglassctl retrieval status
uv run sightglassctl retrieval explain
uv run sightglassctl retrieval rebuild --kind links
uv run sightglassctl retrieval rebuild --kind lexical
uv run sightglassctl retrieval rebuild --kind semantic
```

`status`/`explain` 报告 content-free 的 link/lexical/semantic readiness、indexed/link/pending-message counts、generation/recipe/checkpoint，以及 derived worker 的 running、processed/completed counters、last error 和 storage pause；`explain` 另外报告固定的 ranking 参数。Physical bytes 分类见 `storage explain`。这些 operator 结果不进入 MCP。`rebuild --kind links|lexical` 把对应 receipt 标成 rebuild-pending、原子地 bump generation 并唤醒 worker，重观察后由新 recipe 覆盖；损坏或过期 receipt 因此在重建完成前不会进入查询。`rebuild --kind semantic` 在已配置 lane 上 bump 私有 sidecar generation 并唤醒 semantic worker；后续使用新 namespace，旧候选立即失效，不自动删除远端向量。Disabled 时返回 `QUERY_INVALID`（`reason=index_not_enabled`）。`derived_index_state` 的 generation/version/checkpoint 只在同一短事务内推进。

## Optional BGE-M3 / Vectorize lane

只有一个 active encoder：`@cf/baai/bge-m3`，1024-dimensional float32、cosine，message recipe `sightglass.semantic.bge-m3.message.v2`。缺省关闭；开启会把指定账号/会话当前已 admission 的 canonical text、可用 link-card title/description 与查询文本交给 Cloudflare。资源 bytes、raw transport envelope、数据库/文件路径和原始 account/conversation/sender/message IDs 不进入远端 metadata；用 digest IDs 映射回本地 manifest。URL 若属于正文可能进入 encoder input，不能把 vectors 当作匿名数据。启用需要账号 owner 对 exact conversation list 的外发决定，现有本地只读授权不能替代该决定。

Recipe v2 将完全相同的正文／search text／card fields 只编码一次；空内容与 `unknown` 展示占位文案不编码、不上传、不作为 semantic focus，但依然可以作为 context-only 消息返回。Capture 会越过这些行，coverage 表示已扫描范围，indexed 数只计可表示的输入。Recipe 同时绑定 namespace、input hash 和 cursor state token；v1 sidecar 必须通过下述停止／保留旧目录／从空目录重建的 procedure 升级，不能原地改 recipe 字段或混用旧 vectors。

先由 operator 在自己的 CF account 单独 provision 一个 Sightglass index；不要复用其他产品的 index。以下命令只用于明确授权后的 setup，daemon 不自动执行：

```bash
npx wrangler vectorize create sightglass-semantic --dimensions=1024 --metric=cosine
npx wrangler vectorize create-metadata-index sightglass-semantic --property-name=sent_at --type=number
npx wrangler vectorize create-metadata-index sightglass-semantic --property-name=sender --type=string
npx wrangler vectorize create-metadata-index sightglass-semantic --property-name=kind --type=string
npx wrangler vectorize create-metadata-index sightglass-semantic --property-name=watermark --type=number
npx wrangler vectorize create-metadata-index sightglass-semantic --property-name=has_link --type=boolean
npx wrangler vectorize create-metadata-index sightglass-semantic --property-name=conversation --type=string
```

Metadata indexes 必须在 vectors 上传前创建；schema/type 不匹配会 degraded，绝不自动迁移。见 [Cloudflare metadata filtering](https://developers.cloudflare.com/vectorize/reference/metadata-filtering/)。API token 需要目标 account 的 Workers AI 调用与 Vectorize read/write 权限；运行中的 Sightglass 不从 Wrangler 自动取 token。

停止 daemon 后准备 owner-private `0600` settings JSON（示例值均为 placeholders）：

```json
{
  "enabled": true,
  "external_data_authorized": true,
  "cf_account_id": "<32-lowercase-hex-account-id>",
  "index_name": "sightglass-semantic",
  "source_account_id": "<one-local-opaque-account-id>",
  "conversation_ids": ["<one-authorized-opaque-conversation-id>"],
  "timeout_seconds": 8
}
```

```bash
uv run sightglassctl retrieval configure-semantic --settings-file "$PRIVATE_SETTINGS" --authorize-external-data
uv run sightglassctl retrieval import-semantic-token --token-file "$PRIVATE_TOKEN_FILE"
# Alternatively pipe the token through stdin; never place the token in argv.
uv run sightglassctl retrieval import-semantic-token --stdin
```

两个 enrollment commands 都要求 daemon stopped；返回 `activated=false`，保存 config/Keychain 不是启动或切换 runtime。Settings/token file 必须 owner-private、regular、single-link、no-follow；token 不进 config 或 Git。按本 runbook 的原有 daemon 启动流程显式启动后，`retrieval status` 和 `wechat_status(summary).runtime.semantic_worker` 才能确认 live state；缺 credential 时 deterministic retrieval 仍可用，semantic 报 `credential_unavailable`。

独立 worker 用 bounded capture/checkpoints、至多 16 条 encoder batch 与独立 storage reservations 推进。`data_dir/semantic/index.db` 是 private SQLite sidecar，保存确切 input/version identity、原始 float32 vectors、send intents 与 verified manifest。先持久化 intent 再 upsert；接受请求不等于查询可用，只有完整 values/metadata/namespace readback 和本地 canonical fence 通过才 publication。超时或缺 readback 保留 pending/degraded，重启只读确认既有 intent，不盲目 re-encode/re-upload。当前 policy、pause、projection epoch、source account、model/recipe 和 generation 都限制候选，查询 time/sender/kind/watermark 在 ANN 前过滤并在本地重检。

Sidecar 同时绑定 exact Cloudflare account/index 和 local source account。Projection epoch 改变时先禁止旧候选，再自动开启新的 derivative generation；旧远端 namespace 不会被删除。更换 store/source 或发现 sidecar recipe 不匹配时，semantic 报 `sidecar_unavailable`，普通 reads 继续可用。先停止 daemon，保留整个旧 `data_dir/semantic/`（包含 DB/WAL/SHM）到另一个 private 目录，再按正确 config 从新的空 sidecar 重建；不要复制旧 manifest 到另一 index，也不要用删除 `window.db` 解决。`pending` 只统计当前可访问 scope/generation 的 intents，`pending_total` 保留本地全部 intent 数；远端异步发布没有固定完成时间。

`retrieve` 有独立 semantic read slots，remote 子预算 8 秒；网络不可用时返回本地 lanes 并显式 degraded。Continuation 冻结 ANN IDs/receipt，下一页不重跑查询；semantic generation/publication revision 变化可令 cursor stale。Semantic worker 不推进 reader/update/delivery state，不触发 source、voice 或 resource work；`storage explain` 按 `semantic_sidecar` 报本地物理 bytes，远端 storage/billing 不计入本地 quota。

关闭时停止 daemon，再执行 `uv run sightglassctl retrieval disable-semantic`，按原流程重新启动。Disable 保留 sidecar/config scope/remote vectors；它既不擦除远端数据，也不撤销已发生的外发。远端清理须另行明确授权并由 operator 在 CF 执行。Rebuild 也不会删除旧 namespaces；不要删除 `window.db` 来重建 semantic derivative。

## Cache and identity corrections

```bash
uv run sightglassctl cache status
uv run sightglassctl cache cleanup
uv run sightglassctl cache cleanup --apply

uv run sightglassctl alias set wxperson_... '本地别名'
uv run sightglassctl alias unset wxperson_...
uv run sightglassctl correction merge wxperson_source wxperson_target
uv run sightglassctl correction split wxcorrection_...
uv run sightglassctl correction rebind wxsourcekey_... wxperson_target
uv run sightglassctl correction rollback wxcorrection_...
uv run sightglassctl correction list
```

Cache cleanup 默认 dry-run；`--apply` 只删除 `resource-cache/objects/` 内 liveness 检查确认无任何引用的对象：活跃引用覆盖 `resource_bindings.object_digest`、`resource_derivations.derived_digest`、`voice_jobs.input_digest`/`result_digest` 与 `voice_batch_events.result_digest` 全部 FK 列。删除在事务内逐行复检后先 commit row；最终 inode/path revalidation、row-absence check 与 unlink 位于一个不承担 DELETE 的短 maintenance writer transaction 中，使 unlink 与 voice/resource object registration 串行化。普通 resource staging 仍在 writer transaction 外完成 source/processor/CAS 工作；admission 在同一 registration fence 内复检已验证的 private inode，若 cleanup 已回收它，才用已经有界的 staged bytes 重建并再次验证，然后提交 row/binding。嵌套 transaction 中拒绝执行 cleanup。无 DB row 的孤儿对象仅在 regular、single-link、digest-named 且 mtime 早于 24h 时被非递归清扫，新 staged 文件受宽限保护；unlink 失败的残留由后续轮次老化收敛。`status` 的 unbound 统计覆盖上述全部引用；cleanup 候选 `object_count`/`reclaimable_bytes` 是初选统计，实际删除看 `removed_count`，残留孤儿看 `orphan_count`/`orphan_bytes`。Identity correction 只更新当前 projection，并写 append-only ledger；`message_observations` 不被改写。`cache cleanup` 在同一次调用内还会顺带回收 terminal delivery spool：它一次最多处理 500 个 spool 文件（`batch_limit=500`，`has_more` 指示是否还有下一批），在 pending-publication writer fence 内逐条复核 `reader_deliveries.payload_ref`，保留任何仍有 `pending` 引用的 spool（`pending_preserved`），只删除已 `acknowledged`/`expired` 的 terminal spool 或早于 24 小时宽限（`orphan_grace_seconds`）且无引用的孤儿，并在删除后 fsync spool 目录。`cache cleanup` 的返回体里带 `deliveries` 子块。既有 pending delivery 是 immutable snapshot，继续 exact replay；message IDs/anchors 保持稳定。

<a id="local-voice-transcription"></a>

## Voice transcription worker

Daemon 生命周期内有一个 daemon sibling voice worker（`sightglass-voice-worker`）：它只在 `voice_jobs` 里 lease `pending` 作业，把注入的识别器结果通过 `complete`/`fail` 提交，并在每次循环 reclaim 已过期的 lease。提交始终带 owner/fencing token，所以被取代的执行上下文无法写入迟到结果。**默认安装不配置识别器**：worker 以 `enabled=false` 存在且从不运行任何本地 fake recognizer，`wechat_read_transcripts` 仍可读已提交页面，但 wait block 明确报告 `unavailable`，可观测的 `wechat_status(summary).runtime.voice_worker` 也报告 `enabled=false`。生产识别器由 `runtime/voice_setup.py` 按当前配置装配；测试可构造 `SightglassDaemon(..., voice_transcriber=...)` 注入替身，production `main()` 恒为 `None`。

### 真实本地识别器（Apple SpeechAnalyzer）

配置齐备时 daemon 装配的是唯一的真实识别器 `AppleSilkTranscriber`，它按固定四段执行一个 job：

1. **Capture**（`voice/capture.py`）：在 resource service 自身的两段 snapshot 语义内 `read_resource(mode="original")` 取回该 job 的 exact SILK bytes，随后复核 resource 的 active/binding fingerprint/revision、所属 account 与该 job 记录的 account binding，并把字节写入 `voice-work/` 下 0600 的暂存文件（目录 700、`O_NOFOLLOW`、fsync）。revision 未记录、account 不匹配、envelope 之外的任何不一致都 fail closed。
2. **Decode**（`voice/_decode_child.py`）：解码永远在独立子进程 `python -m sightglass.voice._decode_child` 内进行，输入/输出只经继承的 fd 传递。child 自己有输入字节上限、PCM 字节上限、`RLIMIT_FSIZE` 与 0 字节结果视为失败的检查；parent 另有墙钟 timeout、stdout/stderr 上限、以及超时后杀整个 process group 的 watchdog。只接受 SILK V3（可选微信 `\x02` 前缀），其他 envelope、截断、损坏、超限都是 `RESOURCE_BLOCKED`。
3. **Recognize**（`voice/apple.py` + `swift/sightglass-transcribe/`）：每个 job 起一个预编译 helper 子进程，命令行是 `--pcm <私有暂存路径> --locale <BCP-47>`。helper 只消费 final 结果、排除 volatile、不翻译不补词、不补标点，stdout 只输出一行 JSON（`sightglass.voice-transcript.v1`）。环境变量按 allowlist（`HOME/LANG/LC_ALL/PATH/TMPDIR/USER`）传入，并过滤任何形如 KEY/TOKEN/SECRET/PASSWORD/CREDENTIAL 的名字。
4. **Commit**（`voice/apple.py` → `voice/service.py`）：结果连同 content-free provenance（recipe engine、decoder 版本与 envelope、PCM 规格、resource revision、input digest、字节/帧/时长、recognizer locale/asset/model/OS、`derived_transcript` / `translation=false`）一起进既有 content-addressed store，并走同一短事务 `complete` 路径；fencing 仍在服务端复核。无论成功失败，暂存 SILK 与 PCM 都会在 `finally` 内删除。

失败语义：`RESOURCE_BLOCKED` 覆盖缺 decoder extra、缺/不可执行 helper、损坏或截断 SILK、超出上限、语言资产未安装或不受支持——永久结论，job 落 `blocked` 且不自动重试；只有墙钟超时（decode/helper/operation budget）、pause 与 source generation 变化是 retryable。worker 启动时会非递归清扫超过 `DEFAULT_STALE_STAGING_SECONDS`（1 小时）的遗留暂存文件。

生命周期与 source worker 对称：`serve_forever` 在进程锁与 socket 就绪后先 reclaim 遗留 lease 再 start；`operator.pause/resume`、policy 变更等触发的 `_reload` 与 `daemon.shutdown` 先 stop 旧 worker、唤醒 transcript waiter、把旧上下文仍持有的 lease 归还队列（fencing token +1），再以 bounded join（最多 `VOICE_WORKER_STOP_TIMEOUT_SECONDS`，默认 10 秒）确认旧执行上下文退出，candidate services 与 schema 先在旧 worker 停止前构建／probe；确认 quiescence 后才 swap config/tools 并启动 candidate worker，save/start 失败会恢复旧 config/tools 并重启旧 worker。因此 daemon 重启或崩溃后不会等 60 秒 lease 过期：下一条 `pending` 作业立即可被新上下文 lease，而旧上下文即使仍在识别（非合作式停在 10 秒超时）也无法提交。

识别器失败分三类：`RESOURCE_BLOCKED` 是永久结论（job 变 `blocked`，之后同一输入复用该 job、绝不自动重试）；带 `retryable=True` 且有明确失败原因的识别器错误按 0.5 秒起指数退避、最多 3 次尝试后落 `failed`；operation budget 取消/超时（`TRANSCRIBE_TIMEOUT`）与其他异常同样直接落 `failed`，不进入重试。退避只存在于进程内，重启会清掉等待时间，但 `attempt` 上限仍然生效。合作式识别器会在 lease 提前 1 秒的 operation budget 内被取消；worker 从不宣称 job 处于 `running`，因此一次合作式 stop 会留下 `leased` 作业，由 lease 过期或下一次上下文 takeover 回收。

`wechat_read_transcripts` 的等待道不占用 tool slot：daemon 只在两个短 gate 之间 park（默认最多 2 个 waiter，`wechat_status(summary).runtime.transcript_waiters` 报告 `active_waiters`/`max_waiters`/各计数）。调用方断开连接只会释放它自己的 waiter。

### Voice 配置

`config.json` 的 `voice` 节控制 reader 侧是否自动准备语音，缺省全关：

```json
"voice": {
  "enabled": false,
  "policy": "off",
  "language": "auto",
  "open_item_limit": 3,
  "open_duration_ms": 300000,
  "helper_path": "",
  "helper_timeout_seconds": 120
}
```

- `enabled=false`（默认）时 `wechat_read_messages` 的 `voice` 参数一律等价于 `off`：零任务、零 sidecar，response 与未接入 voice 时逐字节一致；`wechat_read_resource(mode="text")` 对 voice 资源维持 `RESOURCE_UNSUPPORTED`。
- `policy` 是 `read_messages` 未显式传 `voice` 时的默认值，取 `auto | cached | off`。
- `language` 同时是 transcript recipe digest 的一部分和传给 Apple helper 的显式 BCP-47 locale；改变它会使缓存 miss，而不是复用旧转写。**它必须是本机已安装 speech asset 的语言**——未安装的语言会 fail closed 成 `RESOURCE_BLOCKED(reason="not_installed")`，不是静默降级。
- `open_item_limit`（≤3）与 `open_duration_ms`（≤300000）是首个批次步的额度上限，直接注入 `VoiceLimits`；未知时长的 voice 项按 120 秒预留，因此默认额度下首步实际 admit 2 项。
- `helper_path` 为空时按 `<data_dir>/voice/sightglass-transcribe` 解析；否则用配置的路径（支持 `~`）。它只被 `probe_helper` 做 content-free 检查（regular、single-link、非空、可执行），路径绝不进入 `status`。
- Paired `--workspace` / `--copy-current` relocation 独立复制默认 helper；显式 `helper_path` 不会改写或复制，operator 需保持该外部依赖可用。默认 helper 的缺失/unsafe 状态不会被伪造成 readiness；首次 activation 的 private-state fence 见上面的 pair 操作说明。
- `helper_timeout_seconds`（1–600，越界回落默认 120）是单个 helper 子进程的墙钟上限；它不会被 helper 自己声称的音频时长放宽。
- 该配置只影响 reader 的**准备**策略；transcription 由 daemon voice worker 执行。`daemon.status` 的 `voice_read` 节回报生效值与 `readiness`（content-free：decoder 名称/是否可用/版本、helper 名称/是否存在/是否可执行、transcriber、`ready`、`blocked_reason`）；路径与任何 key material 都不出现在 readiness 里。

安装与启用：

```bash
uv sync --extra voice                        # SILK→PCM decoder（可选依赖）
bash scripts/compile-voice-helper.sh         # 预编译 Swift helper
uv run sightglassctl daemon stop
# 在 config.json 的 voice 节设置 enabled=true、policy=auto、language=<本机已安装的 BCP-47>
uv run sightglassctl daemon start
uv run sightglassctl status                  # voice_read.readiness 会说明是否 ready 或为何 blocked
```

`compile-voice-helper.sh` 需要 `swiftc`（退出码 2 表示缺失，3 表示编译失败）；它只编译到私有目录，不安装 LaunchAgent、不启动 tunnel、不联网、不下载 speech asset。本机是否已安装某个语言的资产请在 `status` 的 readiness 或一次真实 `wechat_read_transcripts` 结果里确认，不要假设。

目标语言的 speech asset 未安装时，识别会 fail closed 成 `RESOURCE_BLOCKED(reason="not_installed")`——这是永久结论，job 落 `blocked` 且 worker 绝不自动重试。修复分两步，都是显式 operator 动作：

```bash
# 1) 安装资产（helper 的 operator-only 模式，经 Apple AssetInventory 下载；
#    这是 helper 唯一会联网的路径，转写路径本身永不下载）：
"$HOME/Library/Application Support/Sightglass/voice/sightglass-transcribe" \
  --install-assets --locale zh-CN

# 2) 资产装好后，把既有 blocked job 显式归还队列：
uv run sightglassctl voice retry-blocked
```

`voice retry-blocked` 把全部 `blocked` job 置回 `pending`（清 error/lease，fencing token +1，因此 blocked 那一轮的迟到结果永远无法提交），并唤醒 transcript waiter 与 voice worker；提交后立即提示新工作，durable queue 保留兜底恢复。它不做环境检查——operator 确认环境已修复后再执行，否则 job 只会再次落 `blocked`。

配置变更后需要 daemon reload/重启（`operator.pause`/`resume` 等会触发 reload），长驻 stdio bridge 的行为本身不需要重启。

## MCP and ChatGPT tunnel

本地 MCP entrypoint 是 stdio：

```bash
uv run sightglass-mcp
```

ChatGPT 不直接运行本机 stdio。连接拓扑是：

```text
ChatGPT → OpenAI Secure MCP Tunnel → tunnel-client → sightglass-mcp (stdio)
                                               → sightglassd (Unix socket)
```

给 `tunnel-client` 的 MCP command 必须解析为一个可执行文件 token，不能依赖调用方 cwd。checkout 路径不含空格时，可直接使用 `.venv/bin/sightglass-mcp` 的绝对路径；路径含空格时，先在本机建立一个不含空格且不进 repo 的 symlink（例如 `$HOME/.local/bin/sightglass-mcp`），再把 symlink 作为 MCP command。Tunnel 是 outbound-only transport；Sightglass 自身不打开公网或 localhost HTTP listener。Tunnel ID、runtime credential 和配对状态属于 ChatGPT/OpenAI account 与本机 `tunnel-client` 私有状态，不写入 repo 或 Sightglass config；raw runtime credential 保存在 mode-`0600` 的本机 secret 文件中，只由私有 tunnel profile 引用。`tunnel-client` 日志可能记录 tunnel ID，但不得记录 raw credential。

Tunnel 仅在 daemon `ready=true` 且一次真实 stdio MCP initialize/list/call smoke 通过后连接。当前 bridge 的 expected surface 是十三个 tools，包含 `wechat_read_inbox`、`wechat_read_transcripts`、`wechat_find_links`、`wechat_retrieve` 与 `wechat_find_resources`。ChatGPT connector activation、source installation、policy activation、local stdio success、tunnel process 与 named-host live acceptance 是彼此独立的状态。Source/config schema、tool list 或 input schema 更新后，必须重启现有 `tunnel-client`，否则长驻 bridge child 可能继续运行旧代码。

`tunnel-client run ...` 必须由一个会在 operator session 结束后仍保持运行的本机 supervisor 或明确保持打开的 terminal 拥有；不要把一次性 shell 中的短暂 process existence 当作 tunnel live。使用当前 `tunnel-client` 的 managed native runtime 时，先按它自己的 `runtimes connect --help` 提供既有 tunnel/runtime credential reference，再用 `tunnel-client runtimes status <alias>` 验证；缺少 admin/runtime credential 时不要反复重建 remote tunnel，继续使用已经验证的 private profile 与受控本机 process owner。无论哪种 owner，最终都要同时验证 `/healthz`、`/readyz` 与 control-plane poll，而不是只看 PID。

## Production wheel installation and upgrade

Cold startup/health refresh、普通 `status` 与 `doctor` 只读小型 derivative generation/checkpoint 状态；它们的 retrieval `statistics_collected=false`，count 字段为 null，不以零冒充未统计。完整 link/lexical row counts 由 operator `retrieval status|explain` 显式请求，不放在 daemon 的 socket readiness 之前。

Production 使用独立、non-editable wheel environment；checkout 的 `.venv` 只用于开发。保存 exact source commit、wheel、frozen runtime requirements 和安装 manifest 到 Git 外的 private release directory。只保留有用途的当前/rollback release；historical benchmark helpers 与显式 compatibility imports 有各自 caller 时不按年龄删除。不要把相同的 `0.1.0.dev1` version 当成相同 source 的证据，也不要在每次 checkout 更新后隐式改变正在运行的 package。

以下 preparation 不打开已配置账号或 live DB。`RELEASE_DIR` 必须是尚不存在的、绝对 private 路径；为已验证 commit 选定独立目录。需要先在该 commit 的 source 上完成适用检查。示例包含 native/voice runtime extras，不安装 dev tools，也不增加 lockfile 之外的 production dependencies：

```bash
# Set RELEASE_DIR to an absolute private release directory on an available local volume.
umask 077
mkdir -p "$RELEASE_DIR/artifacts"
git rev-parse HEAD > "$RELEASE_DIR/source-commit.txt"
uv export --frozen --extra macos-wechat --extra voice --no-emit-project \
  --output-file "$RELEASE_DIR/requirements.txt"
uv venv "$RELEASE_DIR/.venv"
uv pip sync --python "$RELEASE_DIR/.venv/bin/python" "$RELEASE_DIR/requirements.txt"
uv build --wheel --out-dir "$RELEASE_DIR/artifacts"
uv pip install --python "$RELEASE_DIR/.venv/bin/python" --no-deps \
  "$RELEASE_DIR/artifacts/sightglass-0.1.0.dev1-py3-none-any.whl"
"$RELEASE_DIR/.venv/bin/python" -c 'import sightglass; print(sightglass.__file__)'
"$RELEASE_DIR/.venv/bin/python" examples/synthetic_read.py
```

Import path 必须属于 release environment，不能指向 checkout 的 `src/`。在 private manifest 记录实际 source commit、Python、extras、wheel 和 `editable=false`；wheel/native fixture verification 与真实账号 activation 分别记录。Production interpreter/base Python 也须保持可用；清理时不能删除它依赖的 interpreter。

Promotion 是显式 operator action。先记录原 config、paused 状态、policy 和已安装 package 的可用 rollback 路径，然后用原 production CLI 执行 `pause` → `daemon stop`。同 schema promotion 保持原 DB。Schema v10 conversion 使用下文 compact／paired selection，禁止用首次 open 触发迁移。核对 exact recovery、candidate 和含 private state staging 的实际空间；暂停验收期间不自动 background growth。

大库的 stopped-only backup create/plan/retire 会执行离线 SQLite 完整性检查与 snapshot digest verification，可能需要数分钟。它们不使用普通 reader 的 25 秒 deadline；进程仍健康且处于该校验时，保留原操作，不因没有中间输出重复执行、重启 daemon 或删除待验证文件。

大库 compressed snapshot 默认相邻保存并计入预算。若需移到单独的 private backup directory，目标必须在已经确认加密的本地卷上；先复制 artifact **及其 manifest**、fsync，并用 `sightglass.model.backups.verify_compressed_snapshot` 完整验证 copied bytes，再通过 stopped-only `storage backup retire --artifact ... --ack ...` 的 exact plan 退休相邻副本。保留实际恢复位置、old wheel/config 和恢复步骤；只移动备份不修改 live DB、CAS paths 或 policy。未加密卷不能接收真实账号的 plaintext DB/backup。

使用新 release 的 `sightglassctl daemon start`，核对 schema、ready、workers、storage，以及 configured provider/current policy 未扩大；仅在原实例未暂停时恢复 `resume`。稳定 command links（例如私有 bin directory 中的 `sightglassctl`、`sightglassd`、`sightglass-mcp`）应全部选择同一 release。用 installed MCP command 完成 initialize/list/call smoke，确认十三个 tools、正常 status 和 retrieval/resource 调用后，重启原有 Sightglass tunnel 并检查 health/ready/control-plane poll。不要创建第二个 tunnel 或把其他服务一起 restart。新的 source，不会自动刷新旧的 long-running daemon/bridge。 `ready=true` 只证明可用 read plane；它不证明 link/lexical 历史 backfill 完成。按 generation/checkpoint 与调用自己的 coverage 判断结果；验证一个已知 positive link、一个 admitted message 和一个可解码 resource，旧样本的 cold context 超时须单独保留为未完成验收。Host 可能仍缓存旧 tool metadata；新 stdio 的十三项列表与 named host 实际显示/消费是分别验证的事实。

若启动或验收失败，先停止新 daemon。same-schema 时切回 old wheel/config；paired conversion 时选择整个 old pair（含独立 spool/CAS/token namespace）。传统恢复必须同时选回 schema-compatible runtime，不能让旧 binary 打开 v10 DB。把已验证的 artifact/manifest 复制回原 `window.db` 同目录，用新 CLI 的 `storage backup plan` 得到 fresh restore ack，执行 `storage backup restore --artifact ... --ack ...`，再启动 old installed CLI 并按原 paused 状态恢复。恢复会回到备份时点；应在 promotion 的 paused 验收阶段作出 rollback 决定。Copy verification、restored DB/schema verification、old binary compatibility 和 live readiness 是各自独立的证据。

成功后只清理有明确替代物的 build inputs、旧候选 exports 和可再生成 caches；保留当前 release，以及能实际运行的上一个 release 和 verified recovery point。不要用 `git clean -fdx`，不要删除 live history、pending spool、source/Keychain/config 或未鉴定的文件。Public docs 描述可重复流程；安装专属路径、账号和 receipts 保存在 Git 外。

## Recovery

### Keychain access from the MCP bridge

`Sightglass Keychain secret is unavailable: mcp-reader-token` 发生在 bridge 获取 reader credential、建立 daemon IPC 之前；它不能证明 retrieval 或 source 失败，也不能单凭这条错误区分 item 缺失与当前进程无法访问。先在本机 operator session 做不输出 secret 的检查：

```bash
security find-generic-password \
  -a mcp-reader-token -s com.indeliblevivi.sightglass \
  -w >/dev/null && echo "reader: OK" || echo "reader: UNAVAILABLE"
security find-generic-password \
  -a operator-token -s com.indeliblevivi.sightglass \
  -w >/dev/null && echo "operator: OK" || echo "operator: UNAVAILABLE"
```

两项都可读时，先通过 installed CLI 的 `status` 和 installed MCP command 的 initialize/list/`wechat_status` smoke 验证本地认证，再检查现有 tunnel 的 managed owner 与实际 MCP child release。稳定 executable link 已切换，不代表长驻 child 已加载新 release。本地 smoke 成功而 connector 仍报 Keychain 错误时，在已有 operator 重启授权内重连 **原有 Sightglass alias**，使用原 tunnel ID、原 file credential reference、原 profile 和稳定 installed MCP command：

```bash
# Set these variables from the existing private runtime/profile.
# RUNTIME_KEY_FILE is a file reference; never put the raw credential in argv.
tunnel-client runtimes stop "$TUNNEL_ALIAS"
tunnel-client runtimes connect \
  --alias "$TUNNEL_ALIAS" \
  --tunnel-id "$EXISTING_TUNNEL_ID" \
  --runtime-api-key "file:$RUNTIME_KEY_FILE" \
  --profile "$TUNNEL_PROFILE" \
  --profile-dir "$TUNNEL_PROFILE_DIR" \
  --mcp-command "$INSTALLED_MCP_COMMAND"
tunnel-client runtimes status "$TUNNEL_ALIAS" --json
```

确认 normalized profile、tunnel identity、credential reference 与 MCP command 保持原值，新 child 使用当前 installed release，并复核 health/ready、control-plane poll 和一次真实 connector `wechat_status`。重连后旧 in-flight request 可能 timeout；待 runtime ready 后做一次 fresh call，不能把 transport health 当成 reader authentication 的证据。Runtime status/log 含 private tunnel metadata，留在本机，不提交或直接分享。只重启这一个 transport；健康 daemon 无需同时重启。

若本机 item 也不可读，保留原 config、数据库和 Keychain items，检查系统返回的访问错误；不要用删 config 或重新 `init` 作为修复。当前 CLI 没有 credential repair/rotate 命令，生成新 token 必须同时处理 config token hash 与 daemon authentication，不能只替换一侧。任何重新 enrollment 应单独完成明确的 operator recovery 决策。

认证恢复后仍需检查返回的 `storage.state`：`hard_limit` 会使 reader `ready=false` 并阻止新的 admission，即使 source/retrieval readiness 已为 ready。用 `sightglassctl storage explain` 做只读 quick 诊断；预算调整、历史压缩/退休或数据库重建是独立操作，重连 tunnel 不会解除容量限制。

### Daemon and reader recovery

- `daemon start` 发现已有可认证 daemon 时直接返回其 status，不启动第二份。
- process lock 阻止两个 daemon 同时拥有同一 runtime；crash 后新 daemon 会在取得 lock 后替换 stale socket。
- source worker 的慢 catalog/page read 不应长期持有 `window.db` writer lock；若 operator/read call 得到 `database is locked`，先停止 daemon、确认没有第二进程，再使用包含 concurrency regression 的 current build 重启，不要只放大 SQLite timeout。
- `SERVICE_BUSY` 表示该调用所属的 bounded lane 已满；`details.work_class` 指明 `local_read`、`source_read` 或其他分类，caller 应按 `retry_after_ms` 有界重试。相同参数的并发请求仍会 join single-flight；耗时 resource derivation 可返回带 `reading_token` 的 ordinary `processing` 状态，由 durable worker 接手，而不是挤占 source/local lane。`SERVICE_TIMEOUT` 可来自 server cooperative deadline 或 client IPC response deadline，不等同于 daemon 已退出；正常 server timeout 后 operation 应从 `runtime.operations.active_count` 消失，下一次调用无需 restart 即可继续。`SERVICE_UNAVAILABLE` 才表示 bridge 无法连接本地 daemon。三者都不得被 host 合并成无来源的 `INTERNAL_ERROR`。若连续出现 timeout，先调用 fast `wechat_status(summary, response_profile="diagnostic")` 或 `sightglassctl status` 查看 lanes、per-tool latency/outcome、resource worker/job、writer/receipt/source-worker diagnostics，再检查 tunnel `/readyz`，不要靠重复刷新 plugin 猜测。
- source worker 的 retryable generation/key/shard 错误会 back off 并保留 durable tail/backfill position；修复 source admission 后 restart/resume，不删除 `window.db` 或 job rows。
- 未 ACK update payload 已物化在 private spool；daemon restart 后先 exact replay，再接受 ACK。
- daemon 无法启动时查看 mode-`0600` `sightglassd.log`，再运行 `sightglassctl doctor`；不要把 log 或 private state 提交到 Git。
