# Current state

Updated: 2026-10-07. This page owns current source, candidate and publication status.
Installation authorization, account metrics, host details, receipts and operational
history belong in private operator records outside Git.

## Source and supported scope

- Development preview `0.1.0.dev1`; Python 3.11+, SQLite 3.43+ with FTS5 trigram and `contentless_delete`. No public package or tagged release.
- Thirteen read-only stdio MCP tools communicate with the policy-enforcing daemon over authenticated local IPC. The [MCP contract](MCP-CONTRACT.md) owns arguments, projections and continuation semantics.
- Config v2, source-ID-v2, window schema v10, parser `sightglass.wechat-parser.v2` and native provider `sightglass.macos-wechat.sqlcipher.v6`. Native access supports only WeChat **4.1.13 / build 269602 / arm64** on macOS; one exact account and verified keys require explicit operator authorization.
- Optional local speech recognition requires macOS 26, the compiled helper and installed language assets. Optional semantic recall uses BGE-M3 + Vectorize, is disabled by default, and requires separate exact-scope external-data consent.
- The [complete coverage ledger](IMPLEMENTATION-PLAN.md) retains every accepted M0–M7 outcome. An implemented tranche does not complete that programme.

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
code locations and aggregate handle counts. Final source validation passed **1,194
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
Local PDF page/text reads and actual Apple helper recognition of generated speech
also passed; the helper source matches the installed release's source commit.
These checks do not establish complete history, a new recognition of real audio,
zero-hint discovery quality or login/restart supervision. Installation manifests,
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
automatic jobs. The public `main` branch owns this workflow and runs both portable and macOS
gates on code pushes. Hosted run results belong to GitHub Actions; local validation
and installed-runtime acceptance remain separate evidence.

## Remaining boundaries

- Source history, lexical/link backfill and resident context can be partial. Empty bounded results do not establish global absence. Unindexed native context may need a complete position scan; the five-second busy-source target remains unverified.
- Real thumbnail-to-original transitions and fresh ChatGPT/Codex tool-catalog and cold-task acceptance remain installation-specific checks. Synthetic preview/transcript fidelity does not replace those checks.
- SG-056 remains partial: independent context-quality holdouts, 100k cloud ANN/build/token/cost measurements and real-corpus semantic acceptance remain open. Historical Apple/Qwen benchmark artifacts do not change the active BGE-M3 route.
- Automatic key extraction/refresh, unattended new-shard enrollment, WGO adapters, desktop UI/watch workflows and other native platforms remain outside the implemented surface. AI Search/K2 are comparison candidates, not integrated services.

## Publication

This repository is the maintained public projection at
[IndelibleVivi/sightglass](https://github.com/IndelibleVivi/sightglass).
It begins with a clean current-tree root and has no inherited private Git or Actions
history. The separately maintained private source remains the editing authority;
publication exports its selected tracked tree, with only this publication-state projection adjusted for the public
entry point. Implementation, tests, contracts, rights and provenance remain aligned.

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
