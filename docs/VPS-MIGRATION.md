# Mac edge / Linux core 操作

本页拥有 remote-core 的 repeatable migration、single-owner handoff 与 recovery。
Source/candidate/live 状态以 [current state](current-state.md)为准。这里的命令需要
operator 已授权的 exact installation；文件存在、source tests 或 SSH 可用都不授予
账号外发、停机、activation、credential 更改或 retirement 权限。

Core 是唯一 WindowDB / reader ACK / delivery / processing / MCP owner。Mac edge
只读原微信 source，保留一个 ordered pending spool，经固定 SSH stdio 主动连接 core。
`remote-capture` 不读取 Linux 微信；official Linux client / provider 是独立验证线。
Wire 与 exact terminal 语义见 [Capture protocol](CAPTURE-PROTOCOL.md)，依赖、helper
与 model 预备见 [Linux processing](LINUX-PROCESSING.md)。

## 先建立可恢复的边界

记录当前 noneditable release、active pair/config、原 tunnel ID/profile/credential
reference、source account/conversation egress ceiling、reader policy 与 semantic 状态。
这些均是私有 operator state，留在 Git 外。Reader policy、Mac egress ceiling 和
semantic consent 是三种不同权限；迁移不自动扩大其中任何一种。

检查 source / recovery / target 的容量和实际 encryption。真实 account recovery
必须放在 verified encrypted local volume；0700/0600 不证明 volume encryption。
Linux target 使用已授权的 encrypted storage，key ownership 单独确定：Mac Keychain
持 key 意味着 VPS reboot 后等待 Mac 解锁，但已经解锁的 core 可在 Mac 离线时继续读；
VPS 自持 auto-unlock credential 意味着 disk 与该 credential 一起泄露可解密。
Vault key 不进入 argv、stdout、日志、source tree 或 migration receipt。

保留 usable previous release 和 verified pre-migration recovery。迁移不隐式 retire
任何 pair/recovery，也不删除旧 active DB 来制造 headroom。按 source、compressed
recovery、target exact recovery、candidate、WAL/temp 和 free floor 分别计算预算。
Frozen transfer 从 stopped 原 namespace 直接流向 target，不在 Mac 再做一份 full copy。
Receiver destination 的 parent 必须已在目标 encrypted filesystem 上建立且为
owner-private directory；receiver 只创建 destination 本身，不递归创建任意 parent。

## Source 与 candidate 预备

使用 frozen runtime extras 与 source provenance 安装独立 noneditable wheel。Linux
必须实际 probe SQLite FTS5 trigram/contentless-delete；不能只检查 version string。
先以 generated fixtures 验证 core/edge、13 tools、restart/replay、resource formats、
real SILK / generated speech、whole-job cgroup OOM/timeout 与 reader survival。
不要把 fake helper、DirectRunner 或 host-only status 当成这一步的证据。

停止 real source 前先准备 typed private config：

- `RemoteCaptureSettings` 的 origin descriptor 保留 native v6 interpretation，绑定
  source instance、source account、conversation ceiling、egress revision、stream epoch
  和独立 edge token hash。它不含 native DB/image key。
- Core `SightglassConfig` 选择 `remote-capture`、Linux data / DB / socket namespace、
  `reader_default_view=replica` 和新的 `activation_generation/activation_path`。
- `EdgeSettings` 绑定原 native settings、origin epoch、同一 stream 与 core generation、
  Mac spool、Mac activation record、独立 edge capability 与 fixed SSH identity reference。
  Edge config 不是旧 daemon config；它不含 WindowDB。
- Native keys仍在 Mac Keychain。Linux reader/operator credentials 使用 scoped
  `SIGHTGLASS_SECRETS_DIR` 的 FileSecretStore；不要用普通 shell 输出导出 raw secret。
  Config、token secret、reader principal、policy、residency 与 signed cursor 的兼容
  必须验证；新 Linux speech helper 另行预备，不能执行搬来的 Apple helper。

使用各 typed class 的 `write/save/load` validation，不手工省略必填字段或杜撰 JSON。
`edge-enroll --settings` 只创建新 spool；已有或 lost namespace 不重新 initialize。
Activation record/credential 在 data export 外；guard 不参与普通 namespace copy。

## 停机与 frozen transfer

先停止并禁用 old full runtime 的 supervisor/autostart，再停止旧 bridge/tunnel child。
只处理本 installation；不要 reboot host 或操作无关服务。使用旧 release 的 operator
命令停止原 daemon，并验证 PID/socket/lock。仅 local `flock` 不建立跨 host single owner。
旧 wheel 可能完全不认识新的 activation guard，因此还必须撤销其真实入口、config 和
reader capability。保留可检查的旧 release/recovery，但它们不能继续 admission/ACK。

第一次从 full-local Mac 迁移没有 edge stream；之后迁移 remote core 必须同时停止
edge，导出其 exact state，先解决所有 unreceived pending。Core high-water、pending batch
identity、immutable ACK 和 request terminal 必须属于同一个 committed cut。

下列变量由 private operator manifest 提供，不能复制成公共示例中的真实值。

```bash
# Existing remote deployments only; the edge must be stopped.
"$EDGE_CTL" edge-recovery-inspect --settings "$EDGE_SETTINGS" --output "$EDGE_STATE"

# The current source runtime remains stopped throughout this operation.
# Omit --edge-state on the first full-local -> remote migration.
"$CANDIDATE_CTL" --config "$SOURCE_CONFIG" migration send \
  --host "$TARGET_SSH_ALIAS" --identity-file "$MIGRATION_SSH_KEY" \
  --remote-python "$TARGET_PYTHON" --destination "$ENCRYPTED_FROZEN_ROOT" \
  --edge-state "$EDGE_STATE" --output "$PRIVATE_TRANSFER_RECEIPT"
```

Sender 与 daemon startup 使用同一 stopped lock。Receiver 逐 chunk 校验、fsync，支持
同一 frozen input/destination 的中断续传；changed input 必须选择新 destination。
最终全文件 digest、source revision 和 committed logical cut 通过后才发布 completion。
Receipt 是 0600 private file；stdout 只报告 content-free completion。Transfer 本身不
启动服务、不改 config、不 activate、不 retire。

保存 untouched exact frozen recovery，在 target 同一 filesystem 为 candidate staging
独立复制 mutable namespace。只通过 `relocate_received` 改 canonical delivery/CAS 的
owned path；source binding/observation JSON、rowid、sqlite_sequence、policy/cursor/ACK、
episodes、FTS 与其他 SQL 值保持 exact。随后使用 operator verification：

```bash
# Run on the target, after independently copying the verified inventory into staging.
"$TARGET_PYTHON" - "$CANDIDATE_DB" "$OLD_NAMESPACE" "$NEW_NAMESPACE" <<'PY'
import sys
from pathlib import Path
from sightglass.runtime.migration_state import relocate_received

relocate_received(
    Path(sys.argv[1]),
    old_namespace=Path(sys.argv[2]),
    new_namespace=Path(sys.argv[3]),
)
PY

"$CANDIDATE_CTL" migration verify-relocation \
  --frozen-db "$FROZEN_DB" --candidate-db "$CANDIDATE_DB" \
  --old-namespace "$OLD_NAMESPACE" --new-namespace "$NEW_NAMESPACE" \
  --output "$PRIVATE_PARITY_RECEIPT"
```

该命令逐 table/row/byte 比较，只承认声明的 owned path transformation。不要把 DB
size/hash 或少数 count 当成 semantic parity；不要修改 exact frozen recovery。

## Owner activation 与原 tunnel 路由

在旧入口已实际撤销后，用 stopped operator API `runtime.activation.write_activation`
为 core 和 edge 分别建立 role、actual namespace、host-bound private credential、
generation、counter/predecessor 的 grant。已有 record 的变化需要 exact
`expected_previous`；已 revoked generation 不重新激活。Cross-host continuation
显式提供 predecessor 和递增 counter。它们是本地 fence，不能代替前一步的 old-owner
revocation，也不能证明网络 partition 下远端失效。

Core Unix socket、fixed stdio relay、reader/operator role 与 SSH transport 是独立能力。
Mac SSH key 只允许 exact installed `edge-session` command，使用 pinned known host；
不要授予 shell/port forwarding。启动 core 后再启动已验证 edge；握手 pin
`core_generation`，每次 source/store/send/ACK-release 都重查 ownership。旧 generation
或撤权后 delayed ACK 不能释放 edge pending。

Mac edge 由用户级 LaunchAgent 接管时，验证的是该 managed process 的实际 source
访问权限。前台 provider 初始化成功不能证明 launchd 下也能打开 source；新的
interpreter 可能需要 owner 亲自确认 macOS App Data 访问。保持正在等待的请求，
核实对应的 interpreter 和 source，确认后重新验证握手与一次 bounded fresh capture。
不要用修改 TCC 数据库、扩大 reader policy 或关闭系统保护代替系统确认。

在 target 验证原 tunnel client release 的官方 digest、signature/attestation 和
exact artifact；沿用原 tunnel identity 与 credential reference。先停止 Mac 上同一
tunnel owner，再把其 MCP command 指向 Linux stable installed command。禁止同时
连接两个 owner。Transport ready、MCP auth、13-tool catalog 与 reader admission 是
不同 gates；只看到 `/readyz` 不算 reader acceptance。

验收至少包含 bounded fresh ingestion、lost-response exact replay、source-offline
replica/search/resource/ACK、Mac wake/reconnect、restart recovery，以及原 ChatGPT/Codex
host 的 ordinary-model 使用。提交每次 host run 前实际检查 model；不能用 Pro 代替。
保留 content-free before/after Mac/VPS CPU/RSS/storage 与 owned-process comparison。
最后才按 owner 已批准的 exact inventory retire；保留仍必需的 source app、edge、
usable rollback release 和 verified recovery。全部 live receipts 留在 Git 外。

## Corrupt pending 与 whole-spool loss

停止 core 和 edge。Cached pending identity 只能从 owner-private metadata 导出；不要
读坏 spool body 来猜 batch/sequence，不把 unknown request 自动 ACK，也不 reset floor。

```bash
"$EDGE_CTL" edge-recovery-inspect --settings "$EDGE_SETTINGS" --output "$EDGE_STATE"
"$CORE_CTL" --config "$CORE_CONFIG" capture recovery-plan \
  --edge-state "$EDGE_STATE" --new-epoch "$NEW_STREAM_EPOCH" \
  --next-sequence "$STRICT_NEXT_SEQUENCE" --output "$PRIVATE_LOSS_PLAN"
"$CORE_CTL" --config "$CORE_CONFIG" capture recover \
  --plan-file "$PRIVATE_LOSS_PLAN" --output "$PRIVATE_LOSS_RECEIPT"
"$EDGE_CTL" edge-recover --settings "$EDGE_SETTINGS" --receipt-file "$PRIVATE_LOSS_RECEIPT"
```

Core 已有 immutable accepted/rejected/cancelled/loss ACK 时沿用原值，不改成新的 loss。
尚未接收的 pending 只有 exact 已授权 request 可提交 `epoch_loss`；它不推进 reader ACK。
Target epoch/floor、loss/terminal、request outcome 与 recovery receipt 同一 writer commit；
config rewrite 中断可用同一 exact plan/receipt 重试。Edge 先使用 exact ACK 解决坏 pending，
再应用 monotonic transition；metadata floor 已提交但 config 未写时同样可重试。

Whole-spool loss 没有 cached state，改用 `capture recovery-plan --whole-spool-lost`，
并在 `edge-recover` 加 `--whole-spool-lost`。只有新、空或 exact 已应用 transition 的
spool 可恢复；不得在既有 spool 上重新初始化。新 floor 必须严格大于失去的序列位置。
Operator 要先核实 epoch/sequence/current owner；无法证明时保留 blocked state。

## Rollback 的两个时间点

新 core 尚未产生 authoritative ACK/admission 时，可停止新 owner、撤其 grant/reader/
tunnel 入口，再以新的 generation 恢复 verified old runtime 与 exact pre-cut state。
任何时候都只启动一个 owner。

新 core 已推进 reader ACK、request outcome、delivery 或 projection 后，旧 pre-cut
snapshot 是 recovery point，不能直接成为 active truth。先停止两端，把最新 core
authority 做同样的 exact frozen cut/transfer/parity，保留 latest ACK 与 pending payload，
再以新的 generation 恢复本地 owner。不会自动 partition failover；不能通过重启旧
LaunchAgent、重新接旧 tunnel 或恢复 raw old config 绕过 owner handoff。
