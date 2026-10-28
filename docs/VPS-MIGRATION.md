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

## Mac-held vault automatic recovery

这是需要 operator 明确授权的新常驻行为，不能从 source 支持或一次手动解锁推断。
它沿用现有 Mac Keychain item、已信任且禁止交互的 `emit` helper 和固定 SSH
forced-command capability，不新增 SSH shell 权限，也不把 key 保存在 VPS。
Mac offline、Keychain locked、SSH 不通时仍然不能解锁；controller 记录 content-free
状态并以 60 秒起、最高 900 秒的 backoff 重试，不弹授权提示。

Canonical source 是 `scripts/vault_gateway.py`（Linux）和
`scripts/vault_recovery.py`（Mac）；二者只依赖 Python standard library。
它们是独立安装的 operator helper，不改变 Core wheel、数据库、edge spool 或账号授权。
记录 source commit、安装字节与原入口 before-image 到 Git 外的 operator manifest，
部署前运行 `python -m unittest tests.unit.test_vault_recovery`。

先在 Linux 为已存在的 vault 建立 root-owned `0600` config。以下都是 placeholder，
必须取自该 installation 的已验证记录，不能猜 UUID、创建新 image 或重建文件系统：

```json
{
  "image": "/private/operator/vault.luks",
  "uuid": "11111111-1111-1111-1111-111111111111",
  "mapping": "enrolled-vault",
  "mount": "/private/encrypted-core",
  "marker": "/private/encrypted-core/ownership/deployment-ready.json",
  "services": ["enrolled-core.service", "enrolled-tunnel.service"],
  "lock": "/private/operator/vault.lock"
}
```

保持原 SSH capability/credential/host pinning，把它的 fixed gateway 入口改为
`exec "$VAULT_PYTHON" "$VAULT_GATEWAY" --config "$VAULT_CONFIG"` 的绝对路径形式；
这些值由安装者固定，远端 caller 不可传命令/路径。保留可恢复的旧入口，不新增并行
active implementation。Gateway 验证 image 私有单链常规文件、LUKS UUID、mapping 的
backing image 和实际 mount；foreign mount/mapping 拒绝。只接受三种 stdin：
`sightglass-vault-status-v1\n`、原 manual raw 32-byte key、或 `R` 前缀加 32-byte key。
Config、image 与 lock 必须在 mount 外，避免挂载后更换 lock inode。
响应仅为有限 booleans/schema/error；所有子命令有 deadline，超时终止其 process group。
已提交到 systemd 的服务启动由 systemd 管理，controller 必须以之后的 status 确认。

Mac config 也为 `0600`，state directory 为 `0700`，receipt/lock/inhibit 使用同一
private directory。现有 SSH alias 必须只连这个 fixed capability，并保留独立 key 与
已 pin 的 known host。`keychain_helper` 必须是已经验证可信的非交互 helper；不能
为后台恢复自动调用 `authorize`、更改 ACL 或重新编译一个新 binary 来取得权限。

```json
{
  "ssh_alias": "enrolled-vault-capability",
  "keychain_helper": "/private/operator/trusted-keychain-helper",
  "keychain_account": "existing-item-reference",
  "receipt": "/private/operator/recovery/state.json",
  "lock": "/private/operator/recovery/owner.lock",
  "inhibit": "/private/operator/recovery/inhibit"
}
```

先手动运行 `"$MAC_PYTHON" "$VAULT_RECOVERY" --config "$RECOVERY_CONFIG"`。
Mounted/healthy 状态不读取 Keychain；`services_stopped` 不启动服务；缺 marker 报
`deployment_not_ready`。只有证明卷未挂载后才读 key，经 SSH stdin 发出 automatic
frame，并复查 mount/marker/services。Gateway 在锁内再次检查，若已有其他 owner
挂载，则保留其停机状态。旧 manual 解锁继续保留显式 start 原 services 的行为。

验证后，用用户级 LaunchAgent 指定三个绝对路径组成的 `ProgramArguments`：
`MAC_PYTHON`、`VAULT_RECOVERY`、`--config`、`RECOVERY_CONFIG`。设 `RunAtLoad=true`、
`StartInterval=60`；不要加 `KeepAlive`。stdout/stderr 指向 `/dev/null`，有界的
last-state JSON 是诊断入口，不为每次 probe 积累日志。读取 config 中 `receipt`
验证实际 managed invocation；agent loaded 不等于已恢复服务。失败不记录异常内容、
命令输出、路径、Keychain reference 或 key。Controller 不读取微信数据。

**维护与手动恢复：** 有意锁卷前，先创建 config 中的 `inhibit` 文件并停用该
LaunchAgent，确认已运行的 invocation 退出，再停止原 services/unmount/lock。
单独创建 inhibit 不能撤回已经发送的 unlock。仅停止 mounted 服务不触发自动
restart。维护完成后移除 inhibit 并重新启用 agent；若 Keychain 不可非交互读取，
在交互前台使用原有 manual unlock/authorization 操作，再检查 Core ready、原 tunnel
health/control-plane poll 和一次插件读取。不要用恢复旧 DB、清 spool 或放宽授权
来替代恢复挂载。回退 helper 时先停用 controller，恢复记录的旧 fixed 入口；
不撤销已有 reader state，不重新初始化 vault。完整 VPS reboot drill 另需授权，
不能为测试这个任务重启无关服务。

保留 usable previous release 和 verified pre-migration recovery。迁移不隐式 retire
任何 pair/recovery，也不删除旧 active DB 来制造 headroom。按 source、compressed
recovery、target exact recovery、candidate、WAL/temp 和 free floor 分别计算预算。
Frozen transfer 从 stopped 原 namespace 直接流向 target，不在 Mac 再做一份 full copy。
Receiver destination 的 parent 必须已在目标 encrypted filesystem 上建立且为
owner-private directory；receiver 只创建 destination 本身，不递归创建任意 parent。

## 容量维护与原卷扩容

Storage soft limit 会暂停后台 derivative/history growth，即使普通读取仍在 hard limit
内可用；先用 content-free status／quick storage explain 区分它与 source/transport
故障。先保留原 recovery，核算 host 实际 free、encrypted filesystem free、owned
footprint 与最终 free floor；调预算不等于增加底层空间，quota 与 filesystem 是两层。

原 loop-file LUKS/ext4 卷的增量维护由 exact installation operator plan 执行：固定
image inode/owner/private mode、LUKS UUID、mapper backing loop/image 和 mount；维护前
抑制 Mac recovery controller，确认 key 可非交互使用，停止 Core/tunnel 并确认 writer
退出。保留 LUKS header/keyslot backup、配置 before-image 和冻结数据库的完整 byte
identity。后者用于扩容前后比较，不能替代已有 recovery。

按 image suffix allocation → bound loop capacity → existing LUKS mapping → ext4
filesystem 的顺序推进，阶段 receipt 放在 mount 外以便失败后检查。每步只操作已绑定
对象，key 继续经安全 stdin，不能写入 key 文件或 argv。卸载后只读 `e2fsck -f -n`
可检查一致性，却不会记录 resize2fs 所需的已完成检查状态；在检查确认干净后，按已授权
offline 维护执行一次可记录完成状态的 `e2fsck -f -p`，再 resize2fs 并重新只读检查。
若 image/mapper 已增大、filesystem 尚未增大，先核对现有阶段后续进，不重复增加 image。

Remount 后验证原 UUID/header 与冻结 DB byte identity，使用 typed config 仅修改已批准
的 budgets 并保留 free floor，恢复原 Core/tunnel，确认原路径 health 与一次 fresh
读取。最后撤销本操作自己的 inhibit，复验 managed recovery。原 key、授权、stream、
Mac edge process 和全部旧 recovery 不因扩容而变更或 retirement。实际命令、绑定、
before-images 与阶段 receipts 属于 Git 外的 operator installation record。

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
访问权限。前台 provider 初始化成功不能证明 launchd 下也能打开 source。对于另一个
developer 的受保护 app data container，macOS 的即时访问确认只在该 app instance
存续期间有效；即使 interpreter 没变，重启 edge 后仍可能再次询问。参见
[Apple 的 app container protection 说明](https://developer.apple.com/videos/play/wwdc2023/10053/?time=1066)。
保持正在等待的请求，核实对应的 interpreter 和 source，由 owner 亲自确认，然后
重新验证握手与一次 bounded fresh capture。不能从一次确认推断 unattended restart
已获权限；VPS replica 读取与 ACK 不依赖 edge 在线。不要用修改 TCC 数据库、扩大
reader policy 或关闭系统保护代替系统确认。

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
