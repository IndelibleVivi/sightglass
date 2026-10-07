<p align="center"><img src="docs/assets/banner.png" alt="Sightglass — your conversations, in view" width="100%"></p>

[简体中文](README.zh-CN.md) · **English** · [Architecture](docs/ARCHITECTURE.md) · [Operations](docs/OPERATIONS.md) · [MCP contract](docs/MCP-CONTRACT.md)

# Sightglass

**Read WeChat where it lives. Bring only the requested context to your reader.**

Sightglass is an experimental, local-first, read-only WeChat reading service for macOS. It gives an authorized MCP client a structured view of conversations, participants, messages and local attachments, with explicit coverage and source receipts. The daemon does not generate summaries. Optional semantic recall uses Cloudflare Workers AI and Vectorize only after explicit operator configuration and external-data authorization; it is disabled by default.

Use it when a reader needs to inspect the original conversation, follow one participant, open a referenced file, or continue from an acknowledged reading position. The source remains on your Mac; the content returned by a tool call goes to the connected client and is subject to that client's data handling.

> **Development preview · `0.1.0.dev1`.** Native access currently supports only WeChat **4.1.13 / build 269602 / arm64** and requires operator-supplied, verified database keys. Sightglass does not extract those keys. Try the synthetic example before configuring an account. Publication and licensing status is recorded in [Current state](docs/current-state.md) and [Notices](NOTICE.md).

## What you can read

| Capability | Behavior |
| --- | --- |
| Conversations and inbox | Paginated, policy-filtered discovery; a materialized activity inbox that remains readable during live-source degradation and reports explicit freshness and coverage. |
| Messages and people | Recent, range, context, single-message and participant-focused reads use the admitted `window.db` projection when its semantic epoch is current. Stable identity stays separate from mutable names. |
| Search | Cursor-free searches prepare a bounded source page within the requested conversation/time scope before strict literal/AND validation of bounded trigram candidates. Continuation resumes the existing scan. Short terms retain the bounded timeline fallback; missing history and partial zero-hit pages remain explicit. |
| Links and candidate contexts | Find locally observed URLs by exact hostname or approximate hints; retrieve ranked same-conversation contexts with nearby links, explicit replies and replayable anchors. These materialized reads report bounded freshness and never advance updates or ACK. Optional BGE-M3/Vectorize semantic recall uses the same canonical and policy checks; failures leave deterministic recall available. |
| Updates | Reader-scoped delivery and ACK. Unacknowledged payloads replay exactly after a restart. |
| Local resources | Policy-bound catalog search plus image previews/originals (including HEIC/TIFF/BMP), PDF pages/text, UTF-8/UTF-16/GB18030 text, audio/video metadata, local video first-frame previews, structured data, Office documents and ZIP inspection. Authorized warm CAS reads do not reopen the source; a cold native miss acquires its exact locator once through a resource-scoped lease before local processing. Missing bytes and previews remain distinguishable from originals. |
| Optional voice | Exact local SILK extraction; on-device transcription through a bounded decoder and Apple's `SpeechAnalyzer`. Derived text is labeled as a transcript. |

The thirteen MCP tools are `wechat_status`, `wechat_find_conversations`, `wechat_read_inbox`, `wechat_find_participants`, `wechat_read_messages`, `wechat_read_transcripts`, `wechat_search_messages`, `wechat_find_links`, `wechat_retrieve`, `wechat_find_resources`, `wechat_list_resources`, `wechat_read_resource`, and `wechat_search_resource_text`. Arguments, schemas and error semantics live in the [MCP contract](docs/MCP-CONTRACT.md). `wechat_find_links`/`wechat_retrieve` are the only tools that return the full observed raw/normalized URL; the ordinary message link projection stays redacted, and no tool ever fetches a URL.

MCP responses default to `response_profile="brief"` (`sightglass.mcp.brief.v1`), retaining content, coverage, freshness and continuation while omitting implementation diagnostics. Compact rows still use `fields` and `people`. New compact recent pages default to 30 messages, search/link pages to 20 hits, and retrieval to 3 contexts; bounded pages target about 16 KiB for reading/retrieval and 8 KiB for search/links. Follow `next_actions` for polling, result pagination or further source scanning; each action identifies its own token field. Preparation continuations reuse their original scope and effective limit. Request `response_profile="diagnostic"` for full receipts or the explicit recovery of truncated message text. Updates retain exact pending-delivery replay and ACK semantics.

A successfully admitted native recent page records its observed window for local rereading; it never advances the background continuous sync frontier over a gap. Known current-epoch messages and their indexed context remain readable without waiting for background tail completion; missing history stays explicit. Default `recent` pages can lag the source. Use `read_messages(refresh=true)` without a cursor for the latest source messages, to fill a partial context or explicitly reread current source content; the same policy and bounded source checks apply. Storage-blocked voice preparation reports `not_scheduled` while preserving the text page. Materialized reader progress can use bounded maintenance space, but required writes still respect the filesystem free floor.

Ordinary daemon search first returns **preparing** with a signed `reading_token`.
Repeat identical arguments with that token (or the existing `cursor` parameter)
to obtain canonically validated results; preparing is never an empty result.
The bounded private job survives restart, re-scanning an interrupted conversation
under fresh source evidence once an identical token poll supplies the query again.
Query text stays in memory. Final result `next_cursor` keeps normal search pagination.
See the [search lifecycle](docs/MCP-CONTRACT.md#asynchronous-search-preparation).

Cold `find_links`/`retrieve` also return preparing, then usable bounded results,
including when cached bodies and indexes were released. Only matches and bounded
context are cached. A partial result supplies a separate continuation `reading_token`
to scan older rows; the original token only polls, and `page.next_cursor` only
paginates prepared results. Traversal across multiple read leases never claims one
complete current snapshot. See the [cold discovery lifecycle](docs/MCP-CONTRACT.md#cold-linkretrieval-preparation).

Retention is an independent operator setting. New configurations and conversations
default to `on_demand`: idle polling opens no message bodies, and foreground search
retains matching candidates rather than every scanned source row. `keep` collects
future messages continuously; historical backfill must be explicitly selected.
`recent` defaults to 30 days and 512 MiB per conversation. On-demand reads cache
bounded pages/context for 24 hours and at most 256 MiB per conversation; the shared
temporary-body cap is 1 GiB. These caps cover resident body copies, while the outer
storage budget covers the whole installation. Legacy admitted stock remains protected
until an exact operator preview/apply releases its disposable copies. Expiry preserves
identity, observed traversal state, corrections and exact pending replay; it never
refills history automatically. Operator commands and the stopped-only schema-v10
candidate/paired rollback path are in [residency operations](docs/OPERATIONS.md#selective-residency-and-offline-compact).

New admissions store ordinary body text once; card search fields retain their
independent meaning. Legacy schema-v10 rows remain readable; explicit stopped-only
compaction normalizes exact duplicate current fields while preserving frozen recovery,
identities and observation episodes. Startup performs no bulk rewrite.

Local link/lexical reconciliation uses eligible resident bodies; releasing a body also
removes its local derivatives, and idle/restarted/rebuilding workers cannot recreate
empty records from durable history. Resource, voice and derived workers wake on new
work, retry or lease deadlines, with a 30-second fallback. Settled on-demand metadata
uses the same slower cadence; keep/recent collection retains its live-tail cadence.
Native catalog handles are reused, with at most 16 idle handles and a 60-second age
threshold; active reads and narrow-session handles retain their own lifetime. These
are local work bounds, not a measured production memory or latency guarantee.

For an explicitly authorized release of all existing body copies, the stopped-only
compact workflow accepts `storage compact preview --all-stock`: a snapshot-bound
plan with streaming membership, while retaining pending deliveries and active job inputs.
An already-current schema can move to another verified private volume with
`storage compact prepare-pair --copy-current`, preserving an independent rollback namespace
without repeating conversion. Both operations require a stopped daemon.

## Try it with synthetic data

With Python **3.11+**, SQLite **3.43+** built with **FTS5 trigram** and
`contentless_delete` support, and [uv](https://docs.astral.sh/uv/) installed:

Clone the repository below for the interface and example documented here:

```bash
git clone https://github.com/IndelibleVivi/sightglass.git
cd sightglass
uv sync --extra dev
uv run python examples/synthetic_read.py
```

The example creates a disposable source and read model, discovers a synthetic group and reads three messages. It does not inspect WeChat, access Keychain, start a daemon or alter an installed configuration. The temporary files are removed when it exits.

```json
{
  "source": "generated synthetic fixture",
  "schema": "sightglass.message-page.v1",
  "returned_messages": 3,
  "source_page_complete": true,
  "message_kinds": ["file", "unknown", "text"]
}
```

`source_page_complete` describes that source page; it is not a claim that an entire account or history has been indexed.

The separately authorized [semantic benchmarks](docs/benchmarks/README.md) can send **generated synthetic text only** to Cloudflare Workers AI and store its vectors in a dedicated Vectorize experiment index. They use no configured account or daemon, download no model weights, and do not enable production semantic retrieval. The active benchmark now uses BGE-M3 only; the two-model artifact is retained historical evidence. Running that experiment consumes the operator's Cloudflare services; ordinary installation and CI never run it.

Optional account-backed semantic retrieval has a separate [operator setup](docs/OPERATIONS.md#optional-bge-m3--vectorize-lane): one dedicated index, exact conversation scope, explicit external-data consent and a Keychain token. Background indexing and query failures report their coverage; disabling the lane retains local/remote derivatives and does not erase previously uploaded data. Semantic recipe v2 deduplicates identical canonical text/card fields and excludes empty inputs and unknown-message placeholders; those messages remain available through context neighbors. Upgrading a v1 sidecar requires the stopped-only derivative replacement procedure in the operator guide.

## Connect a local account

The native runtime requires macOS on Apple Silicon, the exact supported WeChat profile, and a complete key map for the selected account. Optional voice recognition additionally needs **macOS 26**, a Swift toolchain and installed speech assets for the selected language.

```bash
brew install poppler sqlcipher
uv sync --extra dev --extra macos-wechat
```

Follow [Operations](docs/OPERATIONS.md) for initialization, verified key import, policy selection, daemon lifecycle and recovery. Native initialization selects **one readable conversation** by default. Account-wide access is a separate operator decision, with a denylist enforced on the server. New installations use a neutral `Reader` profile; existing saved reader identities remain unchanged.

For a production installation, use the [isolated wheel promotion and rollback procedure](docs/OPERATIONS.md#production-wheel-installation-and-upgrade). Keep its environment separate from the editable development checkout; restart the existing bridge/tunnel after promotion.

Once a development daemon is configured and running, launch the stdio bridge from the checkout:

```bash
uv run sightglass-mcp
```

For a client's command configuration, use the absolute path to the installed `.venv/bin/sightglass-mcp`; do not depend on the client's working directory. Stdout carries MCP only. A remote client needs a separately configured transport; Sightglass itself opens no HTTP listener. External tunnel/host acceptance is installation-specific.

Optional local transcription:

```bash
uv sync --extra dev --extra macos-wechat --extra voice
bash scripts/compile-voice-helper.sh
```

The helper build only compiles a local executable. It does not start a service, enroll keys or download speech assets. Its default destination belongs to the default data directory. For a paired production installation, pass the active configuration's helper path, or `<data_dir>/voice/sightglass-transcribe` when that setting is empty; a helper in another pair does not make the active pair ready. Missing decoder, helper or language assets produce explicit readiness/blocking states; ordinary reads remain available. See the [voice setup procedure](docs/OPERATIONS.md#local-voice-transcription).

## Architecture

[![Sightglass architecture: a thin MCP bridge crosses authenticated local IPC into a policy-enforcing daemon; providers read source data while private projection, replay and resource state stay local.](docs/assets/architecture.svg)](docs/ARCHITECTURE.md)

The daemon owns source access and local state. Providers return evidence; the reader service decides admission and policy. Message bodies in `window.db` are source-derived projections and form the normal foreground read plane for admitted message pages and the native inbox. Materialized responses identify themselves as bounded-stale rather than claiming a new live-source observation. The same database also owns reader ACK/cursor state, correction history and resource/voice bindings; deleting it loses those local states. Local reads, source reads, resource derivation, the single database writer and transcript waits have independent bounded runtime lanes, so slow source or processor work cannot consume local-read capacity. Long PDF derivations use durable jobs introduced in schema v6, with lease/fencing recovery, and return a polling token instead of occupying a synchronous request indefinitely. Slow source reads run outside the database writer transaction. Catalog work uses a full validated snapshot; known conversation/message fallbacks and cold resources use typed dependency-scoped sessions that pin and revalidate only the SQLCipher databases/files they actually read. Cold resource processors run after that source lease closes, and a short transaction rechecks both the resolver revision and any worker fence before binding derived objects. Authorized CAS hits and transcript reads stay local while rechecking the owning conversation's current policy.

See the [component/evidence map](docs/ARCHITECTURE.md) and [editable topology](docs/assets/architecture.mmd).

## Boundaries and limits

- **Read-only at the source.** No sending, recalling, marking as read, app re-signing, injection or WeChat writes. Sightglass writes its own private projection, cache, delivery and transcript state.
- **Explicit local authorization.** Reader identity is injected by the daemon, never supplied in tool arguments. Operator mutations use a separate credential.
- **Bounded disclosure.** Raw database keys, source paths and transport envelopes stay private. Message-bound resource IDs are reauthorized before content is returned.
- **Selective residency.** `keep`, `recent` and `on_demand` do not grant access. Changing a mode does not release legacy stock. Temporary-body expiry can stale a local cursor; repeat the bounded read or explicitly rebaseline updates. Active resource/voice dependencies and pending replay stay protected. Releasing copies does not immediately shrink SQLite's file; physical compaction is an explicit stopped-only operation, and normal startup refuses older schemas.
- **Full observed URLs in discovery.** `wechat_find_links` and `wechat_retrieve` can return credentials, ports, query strings and fragments present in an observed URL. That content reaches the connected MCP client. Ordinary structured message links stay redacted; neither discovery tool visits the URL.
- **No implicit network retrieval.** Missing local attachments stay missing. Transcript recognition stays on device; speech-asset installation is a separate operator action. A connected MCP client can receive requested content.
- **Storage backpressure, explanation and recovery.** The daemon defaults to a 4 GiB soft budget, 6 GiB hard budget, 2 GiB filesystem free floor and an additional 256 MiB maintenance reserve. Soft pressure pauses backfill and new voice work; hard pressure rejects new admission with `STORAGE_PRESSURE`. Filesystem headroom also reflects other applications and macOS; daemon `ready=true` and a healthy tunnel do not establish reader admission. Check `storage.admission_allowed` in `sightglassctl status` and an actual host read when accepting an installation. Pending deliveries still replay, and validated ACKs can use the reserve. New observation payloads use lossless compression; legacy admitted stock remains protected until exact operator release; temporary body copies follow their explicit expiry/cap lifecycle. The read-only operator command `sightglassctl storage explain` returns a quick tracked-file/capacity summary and exact 7/30-day growth when those daily baselines exist. Explicit `--deep` phases inspect exact counts and SQLite physical usage under a 10-second default deadline (25-second maximum), cancel on client disconnect, and retain completed results when interrupted, without exposing content or performing cleanup. Stopped-only `storage backup plan|create|retire|restore` commands create and verify an adjacent private zstd recovery point, and require the exact current plan acknowledgement before destructive retirement or restore. Restore uses a durable rollback journal and recovers interrupted DB/sidecar swaps before opening the database. Operator cache cleanup also collects acknowledged/expired delivery spools and aged unreferenced spools while protecting pending replay. The daemon stores at most 64 content-free daily snapshots in a private sidecar for growth comparison. These are admission limits, not an OS-enforced quota. See [storage operations](docs/OPERATIONS.md#storage-budget-and-maintenance).
- **Partial means partial.** Unreadable shards, source changes, incomplete indexes, missing keys and unavailable processors produce explicit coverage or errors. Already admitted current-epoch messages, the native inbox and authorized CAS hits can remain readable with bounded-stale receipts while live refresh is degraded. Bounded absence does not prove deletion or global nonexistence. Validated read windows are distinct from the continuous sync frontier; local contexts preserve gap evidence, legacy completeness is revalidated in bounded work, and an empty filtered page can still carry a signed scan continuation.
- **Narrow native compatibility.** Other builds, key rotation and new shards require supported-profile/key enrollment work. Automatic key extraction/refresh, arbitrary video transcoding, a desktop UI and WGO runtime adapters are not implemented.

Private directories use `0700`, data files `0600`, source databases use read-only SQLCipher handles, and credentials live in macOS Keychain. Messages and attachments are untrusted data, never instructions. Read the [Security boundary](docs/SECURITY.md) before enabling native access.

## Verify and explore

```bash
uv sync --frozen --extra dev --extra macos-wechat --extra voice
uv run python -m unittest discover -s tests -t . -p 'test_*.py'
uv run python -m compileall -q src tests examples
uv run ruff check src tests examples
uv run pyright
git diff --check
```

Code pushes and PRs run the portable gate. The full macOS gate runs on main
pushes, PRs, or manual `Synthetic source gates` dispatch; a feature-branch push
alone does not run macOS. Documentation-only changes skip automatic CI. GitHub
requires the workflow on the default branch before enabling manual dispatch;
until promotion, use a PR for hosted macOS coverage.

The suite uses generated fixtures, including encrypted SQLCipher databases when the native extra is installed. Hosted gates separate portable Linux synthetic daemon/bridge checks from the full macOS fixture suite; native WeChat access remains macOS-only. Full macOS resource coverage also needs `sips` and Poppler. Real Apple speech testing is opt-in and uses synthesized audio; no real conversations or recordings belong in Git. [Current state](docs/current-state.md) records the verified source scope and remaining limits.

| Reader task | Document |
| --- | --- |
| Install, authorize, operate or recover | [Operations](docs/OPERATIONS.md) |
| Understand components and authority | [Architecture](docs/ARCHITECTURE.md) |
| Understand reading reliability and Cloudflare adoption decisions | [Reading reliability](docs/READING-RELIABILITY.md) |
| Integrate an MCP reader | [MCP contract](docs/MCP-CONTRACT.md) |
| Understand link/context retrieval and its experiments | [Retrieval extension](docs/RETRIEVAL-SPEC.md) · [Synthetic benchmarks](docs/benchmarks/README.md) |
| Understand provider snapshots and identity | [Source adapters](docs/SOURCE-ADAPTER.md) |
| Review privacy and trust boundaries | [Security](docs/SECURITY.md) |
| Inspect the accepted design and remaining scope | [Specification v0.4](docs/SPEC.md) · [Coverage ledger](docs/IMPLEMENTATION-PLAN.md) |
| Review upstream provenance and rights | [Notices](NOTICE.md) · [WGO reuse map](docs/WGO-REUSE-MAP.md) |

English and Chinese README editions carry the same support, privacy and rights contract. The detailed accepted specification is in Chinese; stable technical contracts are linked above.

## Rights and provenance

Software and functional materials use **AGPL-3.0-only**; independent explanatory documentation and visual assets use **CC BY-NC-SA 4.0**. See the [exact licensing scope](LICENSING.md). The software license permits commercial use and paid services, with its source-sharing and notice conditions; the independent-content license has a NonCommercial boundary. [NOTICE.md](NOTICE.md) records the WGO behavioral references, the unlicensed `agent-wechat` revision inspected for media-layout evidence, and the optional SILK dependency boundary. Those upstream and dependency terms remain in force; the repository grants no rights to bundle the unaudited wrapped SILK codec. Licensing does not itself publish this development preview.

Sightglass is an independent project and is not affiliated with or endorsed by Tencent, WeChat, Apple or OpenAI.
