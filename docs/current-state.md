# Current state

Updated: 2026-10-30. This page owns current source, candidate and publication status.
Installation authorization, account metrics, host details, receipts and operational
history belong in private operator records outside Git.

## Source and supported scope

Mac-side connection/control diagnostics from source `8b4f657` passed both hosted
jobs in [40869985781](https://github.com/IndelibleVivi/sightglass/actions/runs/40869985781)
and 101 generated installed Mac tests before activation. Frozen dependencies/native
bytes, source/contracts, original settings/grants and pending spool were preserved;
Core stayed at `4cbe963` at that stage. Live diagnostics then identified repeated
`reply_receive / edge_relay_read_timeout` before source return, without a stop request.

The Edge repair in source
[d44e0a9](https://github.com/IndelibleVivi/sightglass/commit/d44e0a98adff75c81e2fb8e6e46385bd6f1a1430)
replaces the independent one-second control read limit with the already-selected
source deadline and existing short final-exchange drain. It never extends source
work, restarts an expired capture or changes ACK/interpretation. Real duplex-socket
tests reject the old short-read behavior for a delayed legal reply and retain
short-budget expiry, partial reply, explicit stop and exact-spool behavior.
Both hosted jobs in
[40874158926](https://github.com/IndelibleVivi/sightglass/actions/runs/40874158926)
passed: 1,699 macOS tests, 1,044 portable tests, typing/lint/build and both isolated
installed-wheel gates. The frozen Mac candidate passed 105 installed generated tests
with exact 138-module/five-rights provenance and unchanged frozen dependencies,
native binaries, source/contracts and SQLite before activation. Only the Mac Edge
advanced to `d44e0a9`; Core stayed at `4cbe963`, with the original settings, grants,
pending spool and origin. The temporary passive SSH entry probe was restored to its
exact original entry before the final acceptance.

The new Edge initially waited for the operator's macOS App Data confirmation.
After that confirmation, the same process recovered without another restart.
Direct fresh recent/context reads succeeded, and an actual ChatGPT conversation
continued a previously failed contact read and displayed five fresh messages.
A subsequent 150-second window recorded 50/50 connected samples, 14 additional
source-worker polls, no current source error and no increase in Core/Edge connection
errors. These are bounded post-confirmation observations. They do not establish
unattended App Data permission, all-day stability, complete history, newly arrived
message parity or a latency SLO. The displayed result also raised a sender-direction
question; the subsequent native check and source correction are described below.
A complete capture receipt must not be described as complete account history.

### Native direct sender correction: activated

Further same-scope, read-only native point checks established that a direct peer's
same-shard `Name2Id` mapping can coexist with status `3`. The earlier classifier
incorrectly attributed that peer to self; persisted observations and reader
projections faithfully retained the mistake. Source revision `v6.r2` now gives an
exact peer mapping priority over status, keeps unexplained non-peer/nonzero-unmapped
native sender evidence unresolved and preserves the existing status-only compatibility
path only for rows without native sender identity. System and group rules remain
separate. Source
[9e7dae8](https://github.com/IndelibleVivi/sightglass/commit/9e7dae8aca4e5574145d49abb7884745fbedaa02)
passed 1,704 local tests, typing/lint, and both hosted jobs in
[40888630672](https://github.com/IndelibleVivi/sightglass/actions/runs/40888630672).
Its frozen Mac and Linux candidates passed 247 and 358 installed generated tests,
respectively, before activation.

The explicitly authorized stopped transition settled old pending work, independently
matched the Core committed stream position to the empty Edge spool, and verified
encrypted database, semantic-sidecar and spool recovery before changing origin.
Both Core and Edge now use `9e7dae8`, preserving stream and sequence continuity,
schema, account, credentials and authorization scope. Bounded fresh reads through the original
plugin retained message IDs and corrected peer attribution; fresh context also
succeeded. Further progress on the preserved stream was observed after resume. The semantic
worker retains its original exact scope and uses the new generation; old remote
namespaces were not deleted. Native self-username binding remains an open limitation.

This is bounded plugin acceptance, not sustained daily reliability or a new ordinary
ChatGPT browser acceptance: the previously used Chrome control connection was
unavailable during that follow-up. Persistent macOS source-folder permission also
remains a separate implementation and owner-authorization boundary.

- Development preview `0.1.0.dev1`; Python 3.11+ (Linux voice candidate uses 3.12), SQLite 3.43+ with FTS5 trigram and `contentless_delete`. No public package or tagged release.
- Thirteen read-only stdio MCP tools communicate with the policy-enforcing daemon over authenticated local IPC. The [MCP contract](MCP-CONTRACT.md) owns arguments, projections and continuation semantics.
- Config v2, source-ID-v2, window schema v10, parser `sightglass.wechat-parser.v2` and source native implementation `sightglass.macos-wechat.sqlcipher.v6.r2` (source and activated origin). Native interpretation remains v6; implementation revisions feed the existing projection/capture epoch recipe. Native access supports the exact WeChat **4.1.13 / build 269602 / arm64** and **4.1.15 / build 270115 / arm64** profiles on macOS; one exact account and verified keys require explicit operator authorization.
- Optional speech recognition uses macOS 26 with the Apple helper/assets, or an operator-prepared whisper.cpp multilingual model inside an isolated Linux systemd scope. Optional semantic recall uses BGE-M3 + Vectorize, is disabled by default, and requires separate exact-scope external-data consent.
- The [complete coverage ledger](IMPLEMENTATION-PLAN.md) retains every accepted M0–M7 outcome. An implemented tranche does not complete that programme.

## Foreground catalog and persistent source access: source candidates

New remote recent-inbox and conversation-discovery calls now select a catalog
capture when connected. Offline, explicit replica and resident inbox requests stay
local; signed continuations preserve their original plane and fact time. Foreground
and background share the catalog-state admission owner and the existing transaction-bound
receive/ACK path. Metadata refresh neither imports message history nor certifies the
freshness of cached body previews. The independent background catalog cadence remains.
Canonical integration passed 1,754 tests, full Ruff/Pyright and compilation. The
no-background catalog journey covers generated stdio/IPC/Core/Edge calls; signed
continuations also work on hosts that expose only the cursor parameter. This source
candidate has not yet replaced the activated `9e7dae8` Core; frozen installation and
hosted gates are separate next steps.

The optional [macOS source-access helper](SOURCE-ACCESS.md) uses protocol v3,
one exact account-directory grant, authenticated implicit-bookmark transfer and a
lease covering the source lifetime. Preparation is grant-only or a new shadow config;
there is no in-place configuration rewrite or automatic activation. Native origin,
schema, transport, keys and egress policy are unchanged. Generated tests cover the
Swift/Python protocol, fragmented input, nullable fields, correlated errors, scope
and proof failures, before-images and teardown. Non-granting compiled sandbox checks
ran on macOS using a separate generated-test bundle identity. The 29 focused bridge
and Edge tests passed, including a real compiled Swift DTO/parser/encoder harness;
the earlier malformed framing, nullable-field and error-correlation behaviors were
rejected by the new regression. A compiled production-writer probe also covers first
publication, replacement, private permissions and uncertain post-publication failure.
It caught and now rejects the Foundation directory-open failure; publication uses an
explicit directory descriptor and fsync. A real account-directory grant, second-process access and persistence
after restart remain separate owner-authorized acceptance; this helper is not active.

## Vault restart recovery: installed

A confirmed outage occurred after a host restart left the enrolled encrypted Core
volume locked. Existing mount/ownership guards correctly kept Core and tunnel down.
The original Mac-held-key manual recovery restored those services and the original
plugin, without restoring/reinitializing a database or changing grants. This explains
that outage, not every stale or missing-message report.

The explicitly authorized automatic-recovery increment adds standalone standard-library
operator helpers: a finite fixed-SSH vault status/unlock gateway and a Mac launchd
one-shot controller. Healthy probes do not read Keychain; automatic recovery preserves
mounted stopped services, reconfirms the recovered state and supports maintenance
inhibition with bounded backoff. Vault keys remain Mac-held. Generated regressions
cover protocol compatibility, recovery failures, concurrency, deadline process-tree
cleanup, status proof and maintenance races. Source
[c6ac0a1](https://github.com/IndelibleVivi/sightglass/commit/c6ac0a19067f9b2a3e9592f53914da13bdb7863b)
passed both hosted jobs in [40631725869](https://github.com/IndelibleVivi/sightglass/actions/runs/40631725869).
Both helpers were independently installed with exact source-byte provenance. The
existing forced SSH status and successive actual launchd invocations returned healthy;
Core/edge processes, configuration and trusted Keychain helper remained unchanged.
A complete real reboot/locked-Keychain drill was not performed.

Generated native and Core/edge catalog regressions refresh an on-demand conversation's
activity without capturing its body or changing tail progress. No catalog production
defect was established by the current investigation. Fresh recents, opened context,
known-positive ordinary-text search, links and a fresh updates request succeeded in
the original plugin after recovery. These bounded observations do not establish full
daily usability or sustained stability; an older activity time alone proves neither
staleness nor a missing newly arrived message.

## Resident derivative convergence: activated and progressing

The active Core's repeated derivative deadline failures with zero completed batches
exposed an unbounded selection query: output LIMIT followed a full resident traversal
and mismatched sort, and the next attempt repeated the same work. The candidate uses
the existing resident timeline index to select a bounded position page before testing
eligibility/receipts or reading payloads. An internal versioned scan envelope in the
existing state table holds a frozen upper bound and full position. Publication and
scan progress commit together; rebuild has a separate reset generation. Warm pages
advance, restarts resume, completed sweeps revisit earlier holes, and a warm sweep
preserves already-published index readiness. There is no schema/index/sidecar change
or extra source collection.

Generated dense-stock checks reject the old implementation after 20,000 SQLite VM
steps and accept the increment within that bound. Dedicated regressions cover sparse
history, warm pages, actual worker continuation, restart, append, rebuild and failed
publication. Source
[184fd79](https://github.com/IndelibleVivi/sightglass/commit/184fd7959cce9fbe4dc150d3c83ac18b3e1cea80)
passed 1,679 local tests, full Pyright/Ruff/compile and both hosted jobs in
[40634132610](https://github.com/IndelibleVivi/sightglass/actions/runs/40634132610), including
the isolated installed-wheel gates. The actual Linux candidate passed 315 generated
tests with frozen dependency, SQLite and native SILK-byte parity before activation.
The new Core is active with unchanged database/config/grants, schema/interpretation,
Mac edge process and original tunnel identity. A previously issued fresh cursor
continued across the upgrade with five nonduplicated source-confirmed messages.

Live inspection first reached the configured background storage soft limit. The
operator then explicitly authorized growth of the existing encrypted volume and a
budget adjustment, preserving the original key/UUID, free floor and recovery. The
frozen current database remained byte-exact across offline filesystem growth; Core
and the original automatic-recovery controller resumed. Real-stock warm positions
advanced across committed batches without a current error. After the follow-up
activation, the first live warm pass reached `ready`; subsequent samples left its
checkpoint unchanged while the worker idled. No new derivative publication was
observed in that pass. This is not complete source/history coverage. There was
also a post-upgrade disconnected interval followed by a 16-sample connected window
and cleared source-worker error. Repeated transport stability remains open.

## Continued daily reading and lifecycle: active

The activated follow-up replaces the fixed Core stdio proxy's blocked
stdin daemon thread with a bounded nonblocking descriptor loop, and adds finite
connection diagnostics to the existing operator snapshot. Generated subprocess
shutdown, real output backpressure and transient-read tests cover it. Completed
large warm derivative audits are paced hourly; first/rebuild/unfinished work stays
immediate and ordinary admission publishes directly. Source
[4cbe963](https://github.com/IndelibleVivi/sightglass/commit/4cbe9638920a985fb774a6dbb422e0c570745398)
passed 1,687 full local tests, full Pyright/Ruff/compile,
real output-backpressure bounds and an injected `EAGAIN`/`EINTR` receive regression.
Both hosted jobs in
[40640890963](https://github.com/IndelibleVivi/sightglass/actions/runs/40640890963)
passed: 1,687 macOS tests, 1,044 portable tests and both isolated installed-wheel
gates. Its frozen Linux candidate passed 358 generated installed tests and exact
138-module/five-rights provenance checks before activation. The current Core retains
the latest database, configuration/grants, Mac edge process and original tunnel.
The actual fixed-SSH no-hello shutdown probe now exits zero with empty stderr;
the previous proxy had a reproducible Python shutdown fatal in that scenario.

Original-plugin acceptance also exposed an external stdio handshake race: parallel
calls twice returned tunnel-client `409 / mcp_initialization_required`, while a
sibling call succeeded. The existing tunnel was independently configured with its
supported initialized-notification shim, preserving its bindings/credentials and
Core/edge processes. Two subsequent three-call parallel batches passed, including
links and an image preview. This is bounded interoperability evidence, not a
change to Sightglass's MCP protocol or a general availability guarantee; see the
[operator procedure](OPERATIONS.md#tunnel-stdio-initialization).

One fresh cursor attempt after Core activation returned explicit `edge_offline`.
The edge reconnected automatically; that exact cursor then returned five fresh,
nonduplicated messages. Inbox, direct-contact fresh recents, anchored local context
and a known-positive resident text query succeeded. A later ordinary recent selected
replica during another disconnected interval and must not be counted as fresh.
Connection diagnostics retained a finite incomplete-frame failure. At that stage,
intermittent edge disconnects remained unresolved. The subsequently activated Edge
repair above addresses one confirmed control-read timeout cause; it does not erase
the earlier failures or establish sustained availability.

The active Core retains the behavior introduced in `ea97fc7`: ordinary remote updates
connect to their selected fresh capture while
retaining exact local pending/request replay and legacy request compatibility. No
history job or semantic preparation is required for new replies. Producer, capture
and receipt owners must stop before resource/voice lease recovery; failures block
replacement and DB/lock release. Fast operator diagnostics retain bounded content-free
stop/compensation evidence. Candidate receipt writers remain dormant until recovery;
accepted work drains before replacement and failed joins remain retryable.

Final source [ea97fc7](https://github.com/IndelibleVivi/sightglass/commit/ea97fc7d8128c3742f106aa261545f0495d2c037)
passed hosted [40165657188](https://github.com/IndelibleVivi/sightglass/actions/runs/40165657188):
1,643 full macOS tests in 448.277 seconds, 1,000 portable Linux tests in 359.446 seconds,
full Pyright with zero errors/warnings, lint/compile/build and both isolated installed-wheel
gates. The frozen artifact retained all 138 runtime modules, five rights/provenance files
and required Swift/helper/build/contract sources. It passed 243 installed Linux fixture
tests with unchanged frozen dependencies, SQLite and native SILK bytes before activation.

Actual installation exposed a surviving derived worker during pause. Source inspection
found readiness/pending probes outside the batch budget. The final increment includes
those reads in the existing three-second deadline/cancellation, classifies an actual
budget interruption correctly and retains unrelated SQLite errors. Generated real SQLite
long-query tests prove cancellation before replacement. The installed Core then completed
normal pause/resume with every stop and lease-recovery step confirmed, preserving the
latest database, original configuration/grants, native origin, Mac edge process and tunnel.

The original plugin completed direct-contact discovery, ordinary fresh recents and
fresh updates. An unacknowledged message delivery replayed identically across actual
pause/resume, and ACK returned the next fresh delivery. Earlier source-required transport
failure remained explicit; offline results did not claim fresh source confirmation.
This is bounded acceptance, not complete history, proof of a newly arriving human reply,
long-term transport stability or a latency SLO. Generated append/new-reply scenarios cover
the arrival path through real stdio/IPC/Core/edge.

The [SG-061/062](IMPLEMENTATION-PLAN.md) increment is implemented, installed and activated.
Budget-bounded resident traversal is addressed by the activated increment above; its
live traversal has resumed after authorized capacity recovery. Remote cold discovery,
scope/coverage explanations and independent retrieval
quality remain in the [reading programme](READING-RELIABILITY.md).

The remote source grant remains a fixed enrolled conversation set. Automatic
visibility of newly created chats is not implemented, and reader-wide policy does
not enlarge that egress grant. Source-scope continuity and cold queries are genuine
daily-use gaps; the current work does not claim every WeChat conversation/history
is remotely searchable or that the complete product is stable.

## Daily contact reading: source and activation

The current development increment prioritizes ordinary contact recents before deep
history/IR. Default `wechat_read_inbox(mode="recent")` returns
`sightglass.inbox-page.v2`: recent authorized catalog contacts/groups, including
body-free on-demand conversations, executable per-contact read actions, and optional
current-epoch resident previews with explicit availability/time. Conflicted previews
stay hidden. Explicit `mode="resident"` retains the v1 cached-message inbox and
frozen observation behavior. Catalog readability no longer waits for all message tails.

A new ordinary remote `read_messages(mode="recent")` without an explicit view/cursor
uses the connected edge. The daemon freezes the decision before lane classification;
offline replica, explicit replica and signed continuation keep their selected plane.
Selected fresh failure never silently returns cached text. The source/egress policy,
thirteen tools, WindowDB schema v10, native interpretation, transport and dependencies
are unchanged. This adds no background history copying or semantic activation.

Fresh source validation passed all 1,619 tests in 638.535 seconds, full Pyright
(zero errors/warnings), Ruff and compileall. Generated checks cover the actual
stdio → IPC → daemon → Core → edge relay journey, body-free native direct contacts,
policy/pause, catalog cursor/preview changes and transport transitions. The built
wheel passed the isolated frozen noneditable runtime gate: four entrypoints,
thirteen tools, and the public synthetic example listing two conversations before
caching bodies, then reading three messages. The wheel source/license inventory
and sdist Swift/helper/build/contract sources were inspected; neither contains
private runtime state. Source commit `cd82f0b` also passed both jobs in hosted CI
[40125997642](https://github.com/IndelibleVivi/sightglass/actions/runs/40125997642):
1,619 full macOS tests, 967 portable tests, typing/lint/build and both isolated
installed-wheel gates. These are generated-source and artifact results;
the source candidate subsequently passed 200 installed Linux fixture tests with
unchanged frozen runtime, SQLite and native SILK bytes. Core `cd82f0b` was first activated
with the retained Mac edge `066f24e`, the original schema/config/grants and native
interpretation; the continuation increment above subsequently superseded that Core. The original plugin returned body-free direct contacts, ordinary
fresh recent messages, a fresh signed continuation without duplicates, message
detail and anchor-containing materialized context. This is bounded daily-reading
acceptance, not complete history, a new IR consent scope or a long-term latency SLO.

Programme interpretation and next capability order are maintained in
[reading reliability](READING-RELIABILITY.md) and [SG-060](IMPLEMENTATION-PLAN.md).
Remote cold discovery, the unified operator coverage matrix and independent semantic
relevance evidence remain open; they are not prerequisites for everyday contact recents.

## Post-repair review: source and activation

The current source addresses seven confirmed residual/new findings:
selection-time budget and cooperative edge cancellation; nonblocking stdio with
caller leases and unknown-IPC recovery; permanent stale history jobs and fair
retry; revision-proved semantic coverage; bounded native retained-ID/sentinel
consistency; private voice outputs and exception-safe leases; and explicit
stopped-only legacy CAS hardlink recovery. It preserves schema v10, reader ACK,
current policy, residency and exact semantic send-intent fences.

Transport revision 2 is independent of native interpretation. New Core accepts
an old edge with legacy behavior and no remote execution-cancellation guarantee;
new edge requires revision 2. The native r1 fix also needs a deliberate origin
binding transition across Core config, edge config and durable spool. Existing
stream-loss recovery and same-account app `edge-rebind` are not origin migration
tools. Old pending captures must settle under the old exact binding; replacing a
wheel or deleting pending state cannot complete that transition. Old materialized
currentness and signed history progress will be fenced by a new epoch; semantic
indexing then uses a new generation/namespace without remote deletion.

Core `0bd0759` first passed bounded acceptance with the old native origin and
Mac edge `91e0a16`. The separately authorized stopped origin transition activated Core `89bc612`
and Mac edge `066f24e`; the daily-reading increment above preserves that binding. The coordinated cut verified an
empty spool, equal Core/edge stream position and exact last ACK, and retained
verified encrypted database, sidecar and spool recovery. It preserved sequence,
stream, configuration scope and ownership guards while changing the origin.
The installed thirteen-tool stdio catalog, original tunnel, original MCP fresh
recent/signed continuation reads, a bounded older-time source read and generation-2
semantic candidate recall passed acceptance. The old history job failed with its
progress preserved; an explicit same-scope job is running. History and semantic
coverage remain partial. These bounded checks do not establish remote ANN
relevance or the proposed 24/72-hour soak. The measured status-latency
sample is evidence, not a promised 250 ms SLO. Remote cold discovery and a unified
operator coverage matrix remain separate extensions; remote retrieval presently
recalls admitted resident candidates and never proves source-wide absence.

Fresh source validation passed 1,598 full tests, plus two subsequently added
cross-chain caller regressions (actual stdio → authenticated IPC → Core → Unix
edge relay), full Pyright, Ruff and compileall. The cross-chain tests hold terminal
commit behind a barrier to prove no early ACK/spool release, then recover and
complete the next fresh request. This is generated-source process evidence, not
SSH or real-account acceptance. The full suite also caught and corrected an
availability-revision regression: ordinary appends must preserve existing inbox
and materialized timeline cursors while semantic coverage becomes partial.

Implementation `0bd0759` passed both jobs in hosted CI
[39975664045](https://github.com/IndelibleVivi/sightglass/actions/runs/39975664045):
1,600 full macOS tests, 949 portable tests, typing/lint/build and both isolated
installed-wheel gates. Its separately frozen Mac edge candidate passed 175
installed fixture tests without opening a configured source. The Linux installed
suite ran 564 tests with 18 platform skips and found one CAS fixture failure:
immediate unlink/recreation reused the inode and all stat facts on that filesystem,
so the test had not guaranteed its intended different-inode trigger. The fixture
now pins the old inode with an open descriptor and asserts a genuinely different
replacement before checking stale-token rejection. Production code is unchanged;
the corrected Linux module then passed all seven cases and ten replacement repeats.
The other 563 installed results remain separate evidence, not a rerun of that
whole suite. The installed Linux generated-SILK/real whisper.cpp path completed
inside an actual systemd scope with 2 GiB, zero swap, OOM group and reclaimed
workspace; it used no account audio.

The test-only follow-up CI exposed a second fixture criterion error on both
platforms: its global thread-ID equality assertion rejected a baseline thread
finishing normally, with no new survivor. The corrected regression explicitly
exercises that departure, retains Thread objects to prevent ID reuse masking,
rejects new survivors/FD growth and requires a successful fresh read after 200
actual cancellations. Neither fixture correction changes production package bytes.
The subsequent explicit origin-upgrade API is source work with its own ten
regressions; it never runs during normal startup or implies account activation.
The final caller resource check additionally tracks all 402 real AF_UNIX socket
objects through 200 cancellations and a successful fresh call, requiring each to
be closed. It retains bounded global-FD convergence and the original caller
budget rather than adding a separate one-second entry SLO. The combined origin,
capture-boundary/recovery and caller/remote-chain suite passed all 51 tests.

Origin-upgrade candidate `066f24e` passed both jobs in hosted CI
[39984894574](https://github.com/IndelibleVivi/sightglass/actions/runs/39984894574),
including the isolated installed-wheel gates. Its frozen Mac candidate passed
185 installed fixture tests without opening a configured source. A subsequent
pre-activation regression found that revalidating nonempty retained stock into
the current epoch could leave semantic coverage falsely complete when the
observation episode was unchanged. Admission now advances the existing residency
availability revision for that transition, in the same transaction as projection
currentness. First appends retain their original cursor behavior. The new test
failed on the old implementation; all 62 semantic and 44 adjacent residency,
consolidation and fresh-cursor tests passed after the fix. This follow-up is
installed and active on Core `89bc612`. Its hosted CI
[39988990296](https://github.com/IndelibleVivi/sightglass/actions/runs/39988990296)
passed 1,611 full macOS tests, 960 portable tests, full Pyright and both installed-wheel
gates. The frozen Linux candidate passed 565 installed tests (18 platform skips),
with unchanged frozen dependencies, SQLite and native SILK bytes. This acceptance
is separate from the retained Mac candidate and the bounded live checks above.

## Earlier reliability and retrieval repair

The `91e0a16` implementation addresses the reliability review across broker restart and
cancellation, exact terminal replay after policy tightening, frozen delivery ACKs,
runtime reload recovery, inclusive residency pins, durable single-link CAS/delivery
publication, bounded updates, absent semantic-intent retry, Linux voice workspace
recovery, and XML/HTML parser limits. CI now builds and exercises an isolated,
noneditable wheel through `scripts/check-installed-wheel.py`.

Explicit history jobs use their own signed traversal position instead of an old
global coverage floor, so an operator can reacquire previously released bodies.
The existing schema-v10 job cursor holds scope, interpretation and selected logical
generation bindings; committed progress survives restart. Bounded completion does
not certify full history, and expiry does not automatically refill it. The
[operations guide](OPERATIONS.md) covers selected residency, bounded history jobs,
legacy requeue and separately consented semantic configuration.

The source candidate passed 1,545 full generated/native-fixture tests, full
Pyright, Ruff and compileall. Its isolated installed-wheel gate verified 137
package modules, five license/provenance files, four actual command entrypoints,
thirteen stdio tools and a generated three-message read. Hosted CI, the exact
Linux/macOS installation and production activation were verified separately:
both installed owners advanced to `91e0a16`, preserving the current database,
configuration, grants and stream. Hosted CI
[39877969978](https://github.com/IndelibleVivi/sightglass/actions/runs/39877969978)
passed its portable and macOS jobs. This does not establish unattended macOS
App Data access or complete history coverage.
Semantic support remains disabled by default; real-content permission, remote
publication progress and live installation acceptance are private operator facts.

The installed Core subsequently advanced to `0167749`, removing conversation-wide
body-expiry inventory scans from explicit fresh target validation and replica
selection. Automatic cache selection retains its expiry check; replica pages
still enforce per-row resident availability. Its 10,000-body regression bounds
each SQL statement during fresh classification and execution. The 112 adjacent
source tests, 161 installed tests and isolated wheel gate passed; the Mac edge
remains `91e0a16`. The test-only `6872929` follow-up preserves all 137 package
modules and passed both jobs in hosted CI
[39882055429](https://github.com/IndelibleVivi/sightglass/actions/runs/39882055429).

The `c271155` follow-up consumes the caller's remaining operation
budget instead of applying an earlier fixed cutoff while ordered background work
finishes. Original reader/worker deadlines, cancellation and exact terminal
recovery remain in force. A real-relay regression verifies that an in-budget
capture can still be admitted after the unbudgeted fallback interval; deadline
and cancellation regressions retain their existing limits. Its 72 adjacent source
tests, 133 installed tests and isolated wheel gate passed, and hosted CI
[39883146938](https://github.com/IndelibleVivi/sightglass/actions/runs/39883146938)
passed both jobs. Activation preserved the existing database/configuration/grants
and retained the Mac edge. A subsequent fresh signed-page continuation on that
version still exceeded the original reader deadline; that failure is retained
alongside the later acceptance evidence.

The current source retires unselected queued requests when their last waiter
cancels or times out, preserving joined waiters and exact recovery for work already
selected. A captured history step that returns idle/paused now commits exact
rejection immediately. Core status adds content-free in-memory relay phases for
diagnosis. The combined 103 adjacent regressions and scoped Ruff/Pyright passed.
The test-only `d936dde` follow-up synchronizes the Unix relay's independently
published connection state and reaps its proxy on failure; all 137 production
modules match `1dda940`. Hosted CI
[39886041425](https://github.com/IndelibleVivi/sightglass/actions/runs/39886041425)
passed 1,551 full macOS tests (four skips), 901 portable tests (16 skips), full
Pyright, Ruff and both installed-wheel gates. The frozen Linux artifact passed
137 installed tests (15 platform skips) before activation.

The earlier deployment activated Core `d936dde` with compatible Mac edge
`91e0a16`. That promotion preserved the current database, schema, configuration,
authorization, interpretation epoch and semantic setting, and verified thirteen
installed stdio tools and the original tunnel. The original MCP then completed
fresh recent and signed continuation reads, a 200-plus-199-message traversal with
399 unique IDs, and a seven-message context containing its original anchor. All
five reads returned complete rows and fresh receipts without errors or retries
while background history work remained enabled. Content-free relay sampling had
no diagnostic errors. This bounded acceptance does not establish which recovered
branch caused the earlier timeout or a long-term latency guarantee.

The separately authorized installation also verified semantic candidate recall
and resident historical reads through the original MCP. Background history and
remote semantic publication remain partial; no full-history or whole-index
completion is claimed. Exact consent scope, progress, credentials and live
receipts remain private operator records. Unattended macOS App Data access is
still unverified.

## Earlier reliability acceptance (2026-10-21–22)

The current source adds an exact 4.1.15 profile and a stopped-only verified
same-account edge rebind, retaining native settings v2 and the existing
account/instance, credential and stream bindings. Its new profile intentionally
has no unverified sticker-key offset. Source diagnostics cross sealed replay
only through a finite content-free reason set; legacy finite error receipts
remain recoverable. A round-robin scheduling attempt commits with durable
request registration before the source wait, independently of tail, coverage,
reader ACK or later admission. Registration retains the exact request binding
for failure cleanup and late receipt rejection. Rebind page-1
proofs retain descriptor/path identity and generation through the complete
verification pass, rejecting same-name replacement and in-place mutation.

Fresh source cursors now retain their signed view when the caller omits `view`,
including on replica-default cores and existing hosts that expose only
`refresh`. Brief pagination clears `refresh`; context/detail hints select fresh
through that existing argument. Explicit mismatched views, altered signatures,
wrong scopes and locally invalid positions remain rejected before source work.

The broker waits honor the existing caller cancellation
and deadline, so cancelled background waits release foreground priority promptly.
The exact in-flight request stays detached for terminal rejection when its capture
arrives; it is neither reissued nor acknowledged early. A caller-abandoned returned
ticket also receives one prompt exact recovery attempt on the transport owner,
outside the cancelled caller's context, while ordinary active finalization and
failed-recovery replay retain their existing rules. This closes the foreground
handoff and ordered-stream stalls found during native MCP acceptance; it does
not extend timeouts or change the durable terminal-ACK requirement.

The old source reproduces failed-target starvation, deadline starvation and
refresh-only host pagination failures. Three further regressions fail on the
previous blocking broker waits. Generated sustained paging verifies bounded
progress and unique identities. The final source `a5a70cb` passed 56 affected
capture/core/source-worker/host tests, full Pyright, Ruff and compileall. Its
[hosted gates](https://github.com/IndelibleVivi/sightglass/actions/runs/39659370107)
completed 1,465 full macOS tests (four skips) and 815 portable tests (two skips)
successfully, plus full Pyright and Ruff. The noneditable Linux artifact completed
244 installed tests with three platform skips; the retained `4aa8d23` Mac artifact
passed 105 installed tests. Exact module/license payload and frozen-runtime
provenance were verified for both.

The `a5a70cb` core and compatible `4aa8d23` edge are installed and active. The
core-only broker hardening retained the already-authorized Mac edge process.
The current database, configuration, authorization, stream and interpretation
epochs are retained, alongside the previous runtimes. Installed stdio exposes
thirteen tools and the original tunnel passed health, ready and control-plane-poll
checks. The stopped same-account rebind passed complete installed-key verification
and changed only the native profile binding. After the operator resolved macOS
App Data access for the retained edge process, the final core passed ten-page
native traversal with unique identities, a further small fresh read, and original
MCP fresh recent, cursor-only continuation and same-anchor context reads. The MCP
sequence also passed immediately after bulk traversal; resident reads retained
their explicit replica/bounded-stale freshness. An earlier pre-hardening capture
timeout remains part of the evidence.

The original ChatGPT conversation with ordinary GPT-6 at the selected reasoning
intensity passed a complete same-conversation run: discovery, three bounded fresh
pages, same-anchor fresh context, a further small fresh read and a resident read.
There were no tool errors, timeouts or retries. Content-free daemon receipts
independently confirm successful reads in one conversation; fresh receipts stayed
complete and resident freshness stayed bounded-stale. The earlier host run lost
its selected scope between steps and is retained as partial evidence only. These
bounded probes do not establish full-history coverage, one atomic source snapshot
or a long-term latency guarantee. Unattended macOS App Data access after a new
edge process remains unverified. Earlier installation evidence below belongs to
its stated source versions.

## Daily-use reliability follow-up

This section retains the earlier 2026-10-10–11 acceptance. Its runtime selections
were superseded by the current repair above.

The current source separates replica freshness from capture transport evidence:
a connected edge permits an explicit fresh request while source confirmation stays
unknown until capture. Empty resident inboxes guide catalog discovery followed by
one bounded fresh conversation read. They do not trigger history refill or change
on-demand residency. Ordinary native message capture reuses its complete sender
evidence instead of scanning an unrelated participant roster; finite identity
conflict reasons survive sealed transport without exposing arbitrary error details.

Equivalent same-shard ordinary-text copies retain one stable minimum-rowid position
across recent/range, point, context and preparation. Exact raw evidence equality is
required; real conflicts and media ambiguity still fail closed. Duplicate-heavy
windows refill their positions within the same lease instead of losing neighbors.
Schema v10, native v6 and stable message-token formats remain unchanged.

The reliability implementation in `10bb2fe` passed 1,431 local tests, full Pyright,
Ruff/compileall and diff checks, plus focused native/capture/replica checks after
the final cancellation hardening. Its hosted portable and full macOS gates also
passed. A subsequent guidance compatibility check passed 63 contract/replica tests:
the cold conversation action selects fresh through `refresh=true`, without requiring
the redundant `view` argument missing from an existing host catalog.
That follow-up in `20964ac` also passed the
[hosted portable and full macOS gates](https://github.com/IndelibleVivi/sightglass/actions/runs/38055298267)
on public main `7a01036`.

The `10bb2fe` noneditable Linux core and Mac edge candidates passed 206 and 64
installed tests respectively, with exact package/source and frozen-runtime parity.
A follow-up Linux core wheel from commit `20964ac` also passed 206 installed
tests with exact package/source and frozen-runtime parity. That follow-up was
activated under the existing authorization, with schema, configuration, ownership
grants and the latest database retained. Its installed stdio exposes thirteen tools;
the original tunnel passed health, ready and control-plane-poll checks.
The verified Mac edge runtime is selected in its supervisor. After the operator
restored local macOS credential access, it connected and the remaining identity
conflict cleared through validated fresh admission. Controlled default recent,
point, context and backward-page reads succeeded; canonical IDs did not repeat
across pages. The earlier edge runtime and private state are retained.
The original MCP connection verified
empty-resident discovery guidance and bounded catalog discovery against that active
core. During the measured disconnect, its explicit fresh request failed with
`SERVICE_UNAVAILABLE`, while resident reads remained available with `bounded_stale`
freshness.
The original ChatGPT conversation exercised the offline path with ordinary GPT-6
at the selected reasoning intensity: resident reads, explicit fresh failure and
the brief empty-inbox guidance passed. Its explicit profile comparison confirmed
that diagnostic responses retain the domain response without brief guidance.
The time-window follow-up preserves fractional datetime bounds when querying
native integer-second timestamps: lower bounds are inclusive and upper bounds
exclusive. Its encrypted fixture reproduces the previous fractional-bound failure
in both directions and retains exact-second controls. The affected native/capture
suite passed 171 tests, plus full Pyright and Ruff/compileall. Commit `4224374`
passed its [hosted portable and full macOS gates](https://github.com/IndelibleVivi/sightglass/actions/runs/38063371431).
Its noneditable Mac wheel passed the installed edge suite and the added boundary
regression, with exact module/license and frozen-runtime parity. It was activated in
the existing supervisor; the Linux core then retained the compatible `20964ac` source.
After the operator accepted the replacement process's macOS App Data prompt, the
edge connected. Bounded fresh recent, point, context, backward pagination and the
fractional-bound forward time window passed with consistent canonical identities.
The original Codex MCP connection also passed fresh recent and context reads,
with resident reads remaining explicitly `bounded_stale`.
The original ChatGPT conversation completed the same native fresh path with
ordinary GPT-6 at the selected reasoning intensity. Its final single-turn run
passed fresh recent and same-anchor context, with context succeeding on its first
attempt; resident reads retained their replica freshness. An earlier context
attempt failed closed with `SOURCE_GENERATION_CHANGED`; that result is retained
separately and is not rewritten as a success. These bounded checks do not establish
complete history coverage or acceptance of every other capability.
The controlled replacement restart recovered after the owner confirmation;
unattended edge restart permission is not established. Existing configuration,
ownership, database, spool namespace and previous runtimes remain retained.
Optional semantic remains disabled without separate exact-scope
authorization; it is not implied by these local reliability changes. The preceding
installation and hosted evidence below belongs to the earlier source versions.

## Mac edge and Linux core

The remote-core source adds the registered `remote-capture` binding, Linux file-backed
credentials, a bounded Mac capture edge, authenticated outgoing SSH stdio and a
schema-v10 capture journal in the existing writer. It adds no public listener,
arbitrary provider-method RPC, second Mac WindowDB or Linux WeChat source adapter.
Native interpretation remains v6; transport and activation versions are separate.

Remote reader defaults to replica: all thirteen tools retain local policy, opaque
identity, bounded coverage and offline admitted-state access. Explicit fresh calls
use sealed finite evidence; update ACK, body admission, new delivery, request outcome
and terminal receive state commit together. Ordinary source failure rolls them back;
replica ACK remains available offline. Independent bounded reconciliation revisits
old source positions across passes without claiming a complete change stream.
Current revocation/pause takes precedence over the 30-day exact request replay window.

Stopped recovery now handles exact cached corrupt-pending identity and whole-spool
loss with immutable receipts and a monotonic new stream floor. Frozen migration
streams only the owned inventory, verifies an explicit committed logical cut before
relocation, and proves all-table/rowid/sequence parity except declared owned paths.
Activation v2 binds generation, host, actual namespace and a credential outside the
exported data; relay peers pin that generation. Local fences do not revoke an older
installed wheel that predates them. Actual old supervisor, reader and tunnel entry
points must be disabled before granting a new owner.

A noneditable Linux candidate wheel has run real generated-speech SILK encode/decode,
WAV wrapping and whisper.cpp small multilingual q5_1 recognition with the same
production runner. Three approximately 9–10 second Chinese, English and mixed samples
used about 491 MB peak cgroup memory under a 2 GiB ceiling, zero swap, group OOM kill
and two configured recognizer threads. Recognition took approximately 13, 13 and
25 seconds. Chinese had traditional characters and a homophone error; mixed speech
misrecognized the Sightglass proper name while preserving API, 2026 and Zoom.
These results establish a real processing path, not real-account speech quality.
Real PNG/JPEG/TIFF/BMP/WebP/GIF/HEIC previews, PDF page rendering and an MP4 frame
passed. Whole-job 2 GiB OOM, forced whisper OOM and a 2.5-second timeout stopped
their owned systemd scopes while a separate installed reader remained available.

The [public hosted gates](https://github.com/IndelibleVivi/sightglass/actions/runs/37691402170)
passed for commit `a250086`: the full macOS suite completed 1,404 tests with four
skips, and the portable suite completed 767 tests with two skips. Pyright,
Ruff/compileall, package builds and the synthetic example also passed. The installed
Linux wheel completed its separate 836-test gate with four platform/optional-test
skips. The final public candidate's 135 installed Python modules and five
license/provenance files match its verified wheel; `pip check`, CLI help and the
installed synthetic example passed. An actual SSH transfer of a
generated installation preserved pending payload/token bytes and all-table/row
parity after declared path relocation. An actual SSH capture session admitted fresh
messages, then preserved exact request replay, replica reads and ACK after disconnect;
fresh calls failed closed while offline. Its Mac edge namespace had no WindowDB.
These probes used generated data and did not install production SSH credentials.
The official Linux tunnel client candidate
was verified against its release attestation, signed digests, exact archive and SPDX
inventory, then installed separately without activating the original tunnel profile.
These candidate checks do not establish production or ordinary-host acceptance.

The current migration is **activated and accepted for the configured installation**.
The owner-approved inactive local database has been retired as one exact file.
The remaining native enrollment/state, previous usable release and verified local
and target recovery copies are retained. Production
preparation has created
encrypted target storage and provisioned scoped reader/operator/tunnel credentials,
typed configurations, fixed SSH capabilities and supervisor definitions. Native database
and image keys remain in the Mac Keychain. The final Linux speech helper and its
independent model copy have been verified at their intended paths. Persistent
vault-helper access and the fixed SSH unlock capability have passed verification.
The original local core and tunnel have been stopped and their automatic starts
disabled. Encrypted local recovery, the exact frozen installation transfer and
complete target relocation parity have passed verification. The legacy configuration,
reader credentials and local tunnel entry points have been retired. Initial ownership
grants are installed and the Linux core is ready. The original-identity VPS tunnel
has passed health, ready and control-plane-poll checks; authenticated fixed stdio
and a status call through the original Codex connector also passed. After the owner
confirmed macOS App Data access, the managed Mac edge connected and preserved the
native interpretation epoch under the same initial ownership grants. Foreground
provider initialization alone does not establish managed access.

The resource-list correction in `b1c1c28` uses current resident bindings under one
local read snapshot. Its [hosted gates](https://github.com/IndelibleVivi/sightglass/actions/runs/37715145609)
passed 1,410 macOS tests with four skips and 773 portable tests with two skips,
plus full Pyright, Ruff, compileall, builds and the synthetic example. A separate
immutable Linux wheel passed module/license byte verification, frozen dependency
checks and 90 installed replica/resource tests with two skips, then was activated
under the existing grants and database.

Controlled production online checks exercised all thirteen tools, bounded fresh
ingestion and exact replay after discarding the first committed update response.
With the managed Mac edge stopped, resident context matched exactly; search, links,
retrieval, cached resources, resource metadata, exact replay and durable ACK passed.
Explicit fresh access failed closed. The managed edge reconnected after the owner
confirmed the new process's App Data prompt; this does not establish unattended
restart permission. Before/after process and storage samples are recorded privately.

The relay correction in `09dc5d3` marks unfinalized work abandoned, recovers exact
replay through a retryable single-flight rejection, and rechecks ownership inside
the terminal writer. Its [hosted gates](https://github.com/IndelibleVivi/sightglass/actions/runs/37720731261)
passed 1,418 macOS tests with four skips and 781 portable tests with two skips,
plus full Pyright, Ruff, compileall, builds and the synthetic example. An immutable
Linux candidate passed 145 installed capture/relay/replica/resource tests with two
skips and exact module/license, frozen dependency and native SILK byte checks, then
was activated under the existing database, configuration and grants. The same Mac
edge process reconnected; the original tunnel and Codex connector status passed.
Controlled fresh reads and fresh update lost-response replay passed, and the earlier
pending batch's exact durable rejected terminal was verified.

The ordinary-model ChatGPT run exercised all thirteen tools and passed resident,
resource and cached-transcript reads, but fresh revalidation had exposed the
unfinalized ticket. After the relay promotion, the same ordinary model and reasoning
intensity passed the five-step fresh revalidation: ready/unpaused status, fresh
context, a fresh owning message, local resource metadata and a second actual fresh
context call. Content-free backend receipts confirm all five calls; fresh pages
carry their new `fresh_as_of`, while resource metadata retains replica/bounded-stale
semantics. This closes the failed host path without repeating unaffected tools.
Post-retirement checks confirmed the active core, connected edge and original tunnel;
installation-specific inventory, authorization and receipts remain outside Git.
See [VPS migration](VPS-MIGRATION.md),
[Capture protocol](CAPTURE-PROTOCOL.md) and [Linux processing](LINUX-PROCESSING.md).

## Implemented paths

Ordinary current-epoch message/context pages, resident-only inbox and authorized
warm resources use local materialized state with bounded-stale/cache freshness.
Explicit `refresh=true` uses a bounded source lease; validated observed windows
never advance a continuous sync frontier across an unobserved gap. Source reads
validate their recorded dependencies before admission, and canonical policy,
identity, resource-revision and cursor fences remain authoritative.

Search uses bounded asynchronous preparation, signed polling tokens and fresh
canonical result validation. Cold link/context discovery scans bounded physical
candidate pages, admits only matches and bounded context, and offers separate
idempotent source continuation and local result pagination. Query text stays in
memory. Traversal across leases never claims one complete current snapshot.

Schema v10 separates access policy from `keep`, `recent` and `on_demand` residency.
New configurations/conversations default to on-demand: 24 hours / 256 MiB per
conversation, with a 1 GiB shared temporary-body cap; recent defaults to 30 days /
512 MiB per conversation. Expiry preserves durable identity/state and active
resource/voice inputs, protects pending replay, and never refills history.
Legacy stock remains protected until exact operator release preview/apply.

Stopped-only schema 9/10 compaction freezes one committed input for exact compressed
recovery and resumable conversion. Runtime/config/database selection uses independent
spool/CAS/token/sidecar namespaces and a durable recovery journal. Same-schema
`--copy-current` relocation preserves exact bytes without rebuilding the database.
Normal startup refuses legacy schemas; candidate creation, installation, activation
and retirement each require their own applicable operator authority. See
[Operations](OPERATIONS.md#selective-residency-and-offline-compact).

Independent runtime lanes and durable resource/voice jobs bound slow work.
Storage admission, quick/deep diagnostics and compressed backup/restore keep
maintenance and recovery explicit. URLs are extracted locally; only link/retrieval
discovery may return the full observed URL. No tool fetches a URL or missing attachment.

## Local lifecycle convergence

The current source changes share resident eligibility between ordinary reads,
background selection and final link/lexical publication; released skeletons cannot
regrow empty derivatives. Resident conversation discovery, ordinary forward/backward
pages, bounds and readiness probes use the existing resident partial index instead
of released history. Missing/stale receipts or missing FTS rows fail back to canonical
candidate recall, and read-only worker probes can reconcile holes even under a ready
flag. Current text has one stored body source, with
distinct card search documents retained; explicit stopped-only compact normalizes
exact duplicate columns, while startup and exact `--copy-current` relocation do not.
Frozen recovery and observation episodes remain separate from representation changes.
Progress/receipts bind the body transformation version; old-format candidates require
a new target path instead of mixed-version resumption.

Independent worker Events, retry/lease deadlines and a 30-second fallback replace
idle high-frequency polling. Queue notifications wait for the outer commit and
writer release. Shared native catalog handles reserve users before waiting and
retire old identities; only idle handles are subject to the 16-handle/60-second
threshold. Operator diagnostics separate current/historical errors and expose safe
code locations and aggregate handle counts. The pre-migration 2026-10-07 baseline validation passed **1,194
tests in 223.061 seconds**, full Pyright, Ruff and compileall. A separate noneditable
wheel environment passed **76 tests in 30.065 seconds**, including actual daemon/stdio
MCP, generated native, lifecycle and paired activation/crash/rollback checks, plus
the synthetic example. All 114 installed Python modules match the source bytes.
The wheel retains five license/provenance files; the source distribution also
retains the Swift helper and build sources.
Generated [lifecycle/scale and native resource receipts](benchmarks/README.md#resident-lifecycle-and-warm-read-scale)
separate reduced VM work/copies/writes from unproven production RAM or latency.
The [100k full compact/recovery run](benchmarks/README.md#compact-candidate-100kjson)
also passed exact current-hit and durable-state parity: 100,007 admitted fixture
messages, real A→B→A episodes and 1,000-row FTS churn/merge. Its particular
encoding/backend conversion reduced DB bytes from 527,396,864 to 490,516,480;
these generated checks do not establish a real-account footprint or RAM result.
Installation acceptance is recorded separately; no real-account migration or
VPS content egress is claimed by this source publication.

An explicitly authorized same-schema local production promotion on 2026-10-07
passed a separate runtime-only noneditable wheel acceptance (**76 tests in 36.251
seconds**), content-free schema/ready/doctor/storage/worker checks, installed stdio
initialize/list/brief/diagnostic status checks for all thirteen tools, and fresh
Codex connector status through the existing tunnel. The original account/policy/
semantic configuration was preserved. That initial gate covered installation
and status. Subsequent controlled installation checks passed bounded live message
context, link/retrieval continuations, image preview, a cached transcript batch
and PDF metadata through the existing ChatGPT app after refreshing its catalog.
Those initial host checks used Pro and do not establish ordinary-model daily-use
acceptance. A fresh ordinary-model run initially failed with `STORAGE_PRESSURE`;
after volume headroom returned and admission reported `ok`, all the same bounded
host checks passed with the operator-selected ordinary model and reasoning
intensity. Sightglass acceptance must never select Pro.
Local PDF page/text reads and actual Apple helper recognition of generated speech
also passed; the helper source matches the installed release's source commit.
These checks do not establish complete history, a new recognition of real audio,
continuous filesystem headroom, zero-hint discovery quality or login/restart
supervision. Installation manifests,
account/host details and independently verified rollback/recovery receipts remain
private operator records outside Git.

## Verification and review follow-up

The repository-owned synthetic example, encrypted native fixtures, process-level
IPC/restart tests and deterministic resource/voice fixtures are the public proof
paths. Reproducible [benchmark evidence](benchmarks/README.md) records generated
corpora and measurement limits. They do not prove real-account coverage, client
acceptance, cloud relevance or busy-source latency.

The 2026-10-05 follow-up fixes all four reviewed lifecycle regressions: cap
candidates exclude released/protected/active-pinned prefixes before their bounded
limit; a later cold-discovery deadline preserves a durable resumable checkpoint;
native v6 reconciles retained context identities across serving shards and fences
v5 projections; paired relocation independently copies the default speech helper
with private executable provenance. The first MCP pass retains all thirteen tools,
adds default brief / explicit diagnostic output and separate continuation actions,
and lowers new-request defaults. Exact update replay, transcript event draining,
resource blocks and authorized complete URLs retain their contracts.

The 2026-10-05 baseline source validation passed **1,128 tests in 240.488 seconds**, full
Pyright, Ruff and compileall, wheel/sdist build and the disposable synthetic example.
The suite includes actual daemon/stdio subprocess and encrypted native fixtures;
it accesses no real account. Package inspection confirms all 110 Python source
entries match the wheel, the five license/provenance files are present, and the
sdist includes the Swift helper and compile script. README/Operations shell blocks
parse, and public documentation links resolve. Candidate-specific package and
example checks remain separate from these source results.

On identical generated inputs and explicit page sizes, the compact UTF-8 catalog
falls from 16,970 to 14,487 bytes; an eleven-call task falls from 26,455 to 22,865
bytes. Search candidate work stays seven in both runs. These are JSON measurements,
not protocol wire totals or evidence of faster source processing. Catalog argument
fields increase from 102 to 113 because the explicit profile is added to every
tool and two fixed strict arguments leave the catalog; a 12 KiB / 70-field target
has not been reached.

GitHub Actions owns CI. Portable checks run on code pushes and PRs; the full macOS
suite runs on main pushes, PRs and manual dispatch. Documentation-only changes skip
automatic jobs. The published public `main` registers this workflow and runs both portable and
macOS gates on code pushes. Feature-branch pushes run portable checks;
full macOS coverage requires a PR or manual run on a registered default branch.
[Public hosted results](https://github.com/IndelibleVivi/sightglass/actions/workflows/synthetic.yml)
belong to their exact commit and repository; local validation and installed-runtime
acceptance remain separate evidence.

## Remaining boundaries

- Source history, lexical/link backfill and resident context can be partial. Empty bounded results do not establish global absence. Unindexed native context may need a complete position scan; the five-second busy-source target remains unverified.
- Real thumbnail-to-original transitions and fresh ChatGPT/Codex tool-catalog and cold-task acceptance remain installation-specific checks. Synthetic preview/transcript fidelity does not replace those checks.
- SG-056 remains partial: independent context-quality holdouts, 100k cloud ANN/build/token/cost measurements and real-corpus semantic acceptance remain open. Historical Apple/Qwen benchmark artifacts do not change the active BGE-M3 route.
- Automatic key extraction/refresh, unattended new-shard enrollment, WGO adapters, desktop UI/watch workflows and other native platforms remain outside the implemented surface. AI Search/K2 are comparison candidates, not integrated services.

## Publication

As of 2026-10-08, [IndelibleVivi/sightglass](https://github.com/IndelibleVivi/sightglass)
is the canonical editing source and the only push/CI destination. The repository
retains its existing independent public history. The final transfer compared all
309 publication-reviewed tracked paths with one exact source snapshot before the
authority clauses were updated; the former private ancestry and Actions history
remain separate. Future changes are made directly in this public source rather
than maintained as a second projection.

The public tree includes source code, contracts, deterministic synthetic tests,
user/operator guides, generated benchmark receipts and self-contained artwork.
Private continuity, account/runtime state, raw exports, local environments and
generated databases are excluded. Runtime installation, production activation,
real-account authorization and named-host acceptance remain separate from this
source publication. No tag, package-registry distribution or binary release is claimed.

The owner selected AGPL-3.0-only for software/functional materials and CC BY-NC-SA
4.0 for independent explanations/artwork; the [licensing map](../LICENSING.md)
binds their paths. [Notices](../NOTICE.md) and the [WGO reuse map](WGO-REUSE-MAP.md)
preserve the AGPL provenance boundary and separate installed-dependency terms.
