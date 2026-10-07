# Source adapter contract

`WeChatSourceProvider` is the internal source contract. Providers declare a stable `kind`, implementation identity, source mode, platform, incremental capability, resource capability, and key-refresh requirement. `src/sightglass/source/registry.py` is the only runtime selection path; duplicate or descriptor-mismatched registrations fail closed.

The registered kinds are `synthetic`, `macos-wechat` and `remote-capture`.
`remote-capture` is an offline core assembly binding to a declared origin provider;
it is not a Linux WeChat source adapter. The capture/core implementation is a
source candidate. Production migration and real remote content egress have not
been activated. [Current state](current-state.md) owns candidate/release status;
[the capture protocol](CAPTURE-PROTOCOL.md) owns this private transfer boundary.

## Synthetic provider

M0–M4 retains `SyntheticSourceProvider` for deterministic SQLite/message/resource fixtures. `DirectWeChatSourceProvider` remains only as a compatibility alias for current internal tests/callers and must not be used to describe the native source. A synthetic root is admitted only when `source.json` declares schema `sightglass.synthetic-source.v1`.

The manifest owns:

- stable `source_account_key`, self principal and reader timezone;
- catalog filename and completeness;
- expected shard logical key, filename, and generation ID;
- optional resource registry entries with stable `source_resource_key`, relative local filename, and synthetic-only encoding/key metadata;
- per-conversation roster coverage.

Health enumerates every expected shard and registered local resource. A missing/unreadable shard makes source state incomplete. A snapshot binds the canonical full manifest, catalog/shard main files and committed WAL content, plus resource state/digest. At snapshot entry, the provider creates mode-`0600` private SQLite backup copies, verifies the live inventory again, and reads message/catalog state only from those copies. Resource bytes remain at their synthetic source location but are opened through no-follow directory descriptors, bounded and digest-checked against the snapshot, then followed by a final live-binding validation. Every source operation and context exit returns `SOURCE_GENERATION_CHANGED` on drift.

The snapshot validates required synthetic tables/columns and account-global source message identity. Identical cross-shard overlap is deduplicated before sorting/paging; conflicting copies of one stable source message ID fail closed with `duplicate_message_identity_conflict`.

Full-file hashing and private SQLite backup copies are an intentional synthetic proof strategy. The native provider uses a separate SQLCipher/file-generation path.

The synthetic provider never discovers `~/Library`, WGO config, keys, caches, or real WeChat paths. No default root exists.

## Native macOS WeChat provider

`MacOSWeChatSourceProvider` currently supports the exact `4.1.13 / 269602 / arm64` profile. Enrollment discovery verifies the official bundle identifier, running app executable/bundle path, version/build/architecture, readable `db_storage`, and required contact/session files across candidate account roots. Once installed, runtime health validates the exact configured `db_storage` root and its containment/profile facts directly instead of recursively rescanning every account directory. Candidate output is opaque and never includes the source root.

Initialization is explicit and offline with respect to ChatGPT. A new operator-selected key-map file is opened no-follow, bounded to 4 MiB, required to be owner-held/regular/single-link, and checked for stable descriptor/path identity around the read before parsing. Existing installations may instead select `--reuse-installed-keys`, which reads the same binding's Keychain map in-process without an export file. Both paths reverify every current contact/session/message database using the encrypted page-1 HMAC; a partial or invalid set is rejected. Source settings v2 bind the exact supported profile and an installation-private `source_account_binding_id`; new installations derive `source_account_key` and a source-scoped Keychain account from that binding rather than from a database credential. Reinitialization of the same candidate preserves an existing v1/v2 account key and profile binding. Runtime does not read the import file, WGO config/cache, or WGO runtime.

Native state has two distinct identities. Logical generation binds a database's relative role, main-file device/inode and one-way key identity; mutable size/mtime/page-1/WAL fields therefore do not stale a live timeline cursor after an ordinary append. A cursor records the logical generations of the message shards that actually served its conversation rather than inheriting every account shard. Physical validation remains stricter for the in-flight read. `catalog` scope fingerprints the complete admitted encrypted database/WAL set. A typed `conversation`, `message`, or `resource` session instead creates one session-owned SQLCipher handle only for each database it opens, captures its main/WAL revision before pinning an explicit read transaction, checks that revision after opening, records the exact database and returned-file read set, and revalidates the recorded dependency facts plus eligible message-shard routing and the configured app/profile binding on exit. A zero-byte WAL created by a read-only open is equivalent to an absent WAL because it contains no source content. SQLCipher receives a raw key and source salt in-process, opens URI `mode=ro`, enables `query_only`, and validates `sqlite_master` before queries. The provider never creates decrypted database copies. A selected main/WAL commit, selected database replacement, page-1/key binding change, or exact returned-file mutation raises retryable `SOURCE_GENERATION_CHANGED`; an unrelated unopened shard WAL append does not. Full-local live MCP calls retry a retryable whole operation at most three times.

The native catalog joins active `SessionTable` rows, `contact` identities, account-level `Name2Id` history evidence, and every conversation-specific `Msg_<md5(username)>` table across message shards. This includes inactive contacts/history rather than equating “not currently in SessionTable” with absence. A table whose hash cannot be mapped to exactly one source conversation makes catalog coverage incomplete. A catalog snapshot reconciles table placement against the current shard set. Table names, `Name2Id` values and lazily requested table columns are cached only under that message shard's physical and logical generation identity; concurrent readers singleflight the same build. Narrow message routing records the eligible candidate shard membership and the negative fact that an unopened candidate has no declared target table. Exit checks that membership; a changed negative shard is rechecked in a fresh read-only SQLCipher view with before/after physical-revision validation. An unrelated WAL append may pass when the target tables remain absent; target-table appearance, candidate movement/addition or a validation-time mutation fails closed. A new candidate without an enrolled key reports `SOURCE_INCOMPLETE`. A shard opened for target payloads retains its stricter selected main/WAL dependency fence. Per-shard SQL keyset iterators use `(create_time, sort_seq, rowid)` and are lazily merged in the requested direction. Inclusive timestamp bounds expose seeks to an existing `create_time` index, while the exact sequence/rowid predicates preserve timestamp ties, NULL-as-zero sequence ordering and exclusive page boundaries. Sightglass never creates indexes in the source database; this optimization does not promise indexed performance when the source lacks a suitable index. The full source sort key then reconciles the opaque cursor and participant filter under a 20,002-row fail-closed traversal bound. Zstandard-compressed message content is decoded in memory.

`SourceScope.conversations(account, ids)` also permits one explicitly declared
conversation set for bounded canonical verification. It never widens an exact
message lookup to an undeclared conversation. Cached contact labels and metadata
still select the contact database: its pinned session view is recorded before a
cache return, cache provenance must match that view's identity/revision, and a
mismatch causes a reread in the same view. Contact/WAL correction during the
operation fails final validation. Metadata and negative routing dependencies do
not enlarge the serving message-body logical generations used by cursors.

Native context reads use the optional `ContextSourceProvider.read_context` capability under the same conversation session and final selected-dependency validation. The focus is resolved once. Each selected shard uses bounded position seeks only when its actual query plan can use an existing chronology-compatible index; otherwise it streams one unordered position pass and keeps bounded neighbor candidates. Timestamp order is authoritative: `sort_seq` is not assumed to be monotonic with time. The canonical full-key merge reconciles overlaps before choosing neighbors. Every retained server-ID neighbor is also reconciled across all shards serving the conversation, including copies outside the requested radius; a conflict fails closed before resource hydration. Only selected validated neighbors resolve resources. Native implementation v6 fences materialized projections admitted under the earlier context interpretation through the existing projection epoch; it performs no startup bulk migration or history refill. A zero-radius side performs no neighbor query. Zero-side `has_more=false` does not certify a source endpoint, and a focus-only window does not prove full history or advance a sync frontier. Without a suitable source index, an exact cold context still requires scanning that conversation's positions; this path does not promise constant-time or five-second cold reads.

Stable internal message tokens use `conversation + nonzero server_id` when available, otherwise `conversation + logical shard + nonzero local_id + create_time + type`, with row/time/sort/type/payload evidence only as the final fallback. Conflicting copies of one stable identity fail closed; `get_message` resolves the stable fields rather than trusting an old physical row location. The canonical native token builder lives in `source/message_identity.py`. A valid positive appmsg reply server ID can resolve a private target token in the same conversation; retrieval exposes only an admitted canonical target under the same hard scope, while ordinary reply projection retains its unresolved display contract. Only repository-derived opaque IDs cross the MCP boundary. In a group row, canonical member evidence exists only when the same shard's `real_sender_id` maps through `Name2Id.user_name` and the raw sender envelope is the exact `mapped_sender + ":\n"` prefix. That evidence overrides a misleading status value; mismatch or unverifiable envelope remains unresolved, while an outgoing row without a member envelope may resolve to account self. System/recall rows are always non-human events and never become self. The verified prefix is removed from visible text. Current contact remark/nickname is stored as account-scoped `current_only` label evidence; it is not retroactively projected as exact message-time `shown_as`. The native v6 descriptor (parser `sightglass.wechat-parser.v2`) declares complete per-message sender evidence, so ordinary message reads admit their returned senders directly instead of pre-scanning 200 roster messages; explicit participant discovery keeps the bounded roster scan, reuses canonical sender parsing without resolving attachments, and records the latest observed activity for every sender (including pre-seeded self/direct participants). An already-admitted native target can skip the account catalog refresh and use a conversation/message session while preserving current roster and final selected-dependency validation. Providers without that evidence flag retain the original roster-first behavior.

Shared catalog SQLCipher handles have one registry-owned lifecycle: reserve before
waiting on the handle lock, single-flight construction, post-open identity checking,
and same-path old-identity retirement. Only zero-user handles are evicted, by a
16-idle-handle limit and 60-second monotonic age threshold. Physical close runs outside
the registry mutex; active/reserved users release before a retired handle closes.
The source worker sweeps idle handles without opening source files, including paused
cycles. Narrow dependency sessions own/close their pinned handles separately. Operator
status exposes aggregate shared/retiring/idle/building/scoped counts, never keys or paths.

New `window.db` admissions keep ordinary text in `messages.text` and omit its
exact copies from `structured_json.text` and `search_text`; distinct card search
documents and structural fields remain. Shared current-body readers reconstruct
the same message/search/semantic inputs for legacy v10 and normalized rows.
Normal startup does not rewrite stock. Explicit stopped-only compact normalizes
duplicate columns and rejects conflicting embedded text, preserving source identity,
observation episodes, rowids, ACK and exact frozen recovery. Release never reads a
historical observation to recreate a body or derivative.

In full local macOS mode, with the daemon and WeChat running, a bounded background worker keeps authorized live tails indexed before processing at most one resumable historical backfill batch. A native live-tail phase owns one 20-second total budget；generation drift or an exact operation-deadline timeout advances through fresh attempts that narrow the conversation scope from 5 to 2 to 1, stale-tail depth from the service default to 20 then 10, and incremental batch from the service default to 20 then 1. A backfill step reuses the durable source target admitted when its job was queued, rechecks current reader policy, and does not refresh and rewrite the full catalog before every history batch. Its separate 20-second total budget uses fresh 50 / 20 / 1 batch attempts for the same two retryable conditions；stop/foreground cancellation, unrelated errors, and a final one-message timeout still fail closed and preserve the prior durable position. This bounds the time during which an otherwise unrelated WeChat WAL append or one slow conversation can invalidate work while retaining complete persisted rotation across the account. A native poll whose physical shard generations are unchanged no-ops only after every currently policy-permitted current-catalog conversation has an indexed tail. A changed physical generation is recorded as fully admitted only after every permitted catalog tail is caught up, so a bounded slice cannot hide a second catalog-advanced conversation on the next poll. Provider/parser changes derive a separate current-tail projection epoch；each current permitted conversation either receives one bounded tail repair or is explicitly marked with exact `duplicate_message_identity_conflict` degradation. Timeline/search cursors from the old projection become stale, and inbox stays unavailable with `source_projection_refresh_pending` until the current catalog is handled. The admitted inbox then projects only current-catalog rows at the current semantic epoch, excludes the exact degraded conversations, and reports their count instead of letting historical/stale rows unlock or appear in the page. Historical backfill preserves the tail's existing epoch and cannot certify that repair. This does not rewrite full history or mutate an already-spooled pending delivery. Otherwise conversation selection is persisted and rotated so a large catalog is covered across polls；unseen conversations, nonzero-unread conversations, and catalog activity newer than the admitted tail are prioritized without starving round-robin probes. The worker never performs an unbounded synchronous full-account history scan. Foreground tools cancel an active background slice and wait for its explicit quiescence signal before entering source/`window.db`; the worker resumes from durable state after the final foreground reader exits. This expected yield is counted separately from source errors. Sparse native participant reads perform one bounded canonical-position sort, safely push stable group-member IDs into `Name2Id` SQL when possible, and defer resource resolution until a message actually matches. Ordinary multi-shard message pages batch up to 256 cheap positions per shard independently from their one-row lazy payload fetch, then merge in canonical order. This avoids repeating an encrypted full-table position sort per selected message while retaining the payload/resource bound；traversal continues across position batches. Daemon `wechat_status(summary)` uses only the last completed/cold in-memory health snapshot. A direct read of an already admitted native conversation skips the full source catalog scan and opens a conversation/message session that validates the databases it actually uses；its catalog receipt becomes partial when persisted inventory/shard generations no longer prove freshness. Native inbox is the admitted observation view and reports the persisted catalog's freshness；current-source conversation discovery remains the route for a new validated full catalog. Search validates bounded indexed candidates against current source before returning them. Conversation catalog and roster expose their actual coverage. The native v6 provider parses appmsg file/image/video metadata, bounded forwarded-chat records and privacy-safe public link fields. Local files/images can be resolved safely through the existing message-bound resource API；video payloads are probed only when this installation actually exposes the digest-named `msg/video/<YYYY-MM>` convention. When an exact optional `message/media_*.db` key is enrolled, voice resolution maps the conversation through `Name2Id.user_name`, then requires one `VoiceInfo` row matching `chat_name_id`, `local_id`, `svr_id`, and `create_time`; the bounded `voice_data` blob must sniff as SILK and is returned as the original without transcoding. Generic resource `metadata` describes `audio/silk` without decoding it. Missing key/row or ambiguous evidence fails closed. Sticker (local type 47) uses the message XML `emoji@md5` digest strictly as a locator for the local `business/emoticon/Persist|Thumb` cache, never as a plaintext integrity assertion. Cache entries are AES-128-CBC encrypted under the account FileXorKey (`MD5(decimal_uin + username + "EMOTICON")`, the key reused as IV, PKCS#7 padding); the key is derived in memory at provider start from the current account's own local MMKV state behind an exact profile/build gate, validated against local encrypted cache prefixes, and never persisted to config, `window.db`, logs, receipts, or Git. A decrypted image original wins；an opaque or missing original falls back to the message-bound thumbnail. Unavailable derivation, missing entries, and non-image payloads report explicit key-missing, missing, and metadata-only states. Animated WXGF stickers keep the container as the blob original while a bounded Annex-B HEVC stream extracted from the bounded header is decoded through the bundled ffmpeg into a bounded PNG for inspection and preview. No CDN or network fallback exists. New message shards, key rotation, or an unsupported WeChat build fail closed until a new explicit verified import/profile is installed.

## Remote capture binding

The thin Mac edge owns the native provider, Keychain DB/decoder keys, source
sessions and one finite immutable spool. It creates no WindowDB, ReaderService,
processor worker or MCP server. The core owns reader policy, residency, ingestion,
delivery/ACK state, resource processing and public projection. Native image
decryption remains at the edge; source credentials never move to the core.

One typed request prepares one complete bounded operation: catalog, recent,
range, context, exact canonical verification, bounded discovery or one exact
resource. It carries exact IDs and limits, with at most 200 messages, 4 MiB of
metadata and 32 MiB of resource bytes. The independent edge egress ceiling binds
one account and an explicit conversation set. Catalog entries are filtered before
serialization; core ReaderPolicy or installation ownership cannot widen that
ceiling. A natural-language search query stays on the core, which sends only its
bounded canonical candidate IDs for verification.

The edge validates its recorded local dependencies and closes the source lease
before sealing, spool publication or any transport wait. An outgoing restricted
SSH session uses the fixed `sightglassctl edge-session` command, a separate edge
capability and the enrolled core activation generation. The wire carries sealed
operations rather than arbitrary provider-method RPC, SQL or path commands.
`RemoteCaptureProvider` itself is offline and fails source calls; an authorized
core task first obtains a sealed capture, then uses an operation-local
`FrozenCaptureProvider` for the existing local ingestion/projection machinery.
A native origin retains implementation v6, parser v2 and WindowDB schema v10.

Remote core ordinary reads use the explicitly stale local replica. A fresh read
requires a complete, unexpired, scope-matching capture and fails when the edge
cannot confirm it. A bounded missing ID is absence only within that operation;
it is never deletion/recall or whole-history evidence. Each operation has its own
finite dependency fence and freshness time. A range, prefix, continuation or
reconciliation pass assembled from several operations does not establish one
global source mutation snapshot. Current policy and reader authority are checked
again even when an exact durable request outcome can replay locally.

The official Linux WeChat native source adapter (P6), including its enrollment,
login, key/history and coexistence paths, is not implemented. The implemented
Linux core resource/ASR candidate uses a Mac-origin capture; its actual target
gates are separate from portable fixture evidence. See
[Linux processing](LINUX-PROCESSING.md) and [operator procedures](OPERATIONS.md).

## Bounded search preparation

In full local mode, `prepare_search_page` yields bounded `SourcePreparationStep`
evidence under one conversation lease. Native positions use rowid keyset batches (at most 1,024),
Python time/position filtering and bounded canonical-order heaps; only selected
messages reach payload/resource parsing. Rowid traversal is physical progress,
not chronological evidence. Each step checks operation cancellation and current
reader authority. Only the complete page may enter ordinary admission, with the
lease's final selected main/WAL validation before commit. An interrupted lease
cannot survive restart; its conversation re-scans from zero. Synchronous range
and recent methods remain canonical callers for ordinary message refresh and
library fixture reads. `search_generation_binding` selects the target's logical
message-shard generations without reading message rows and works in a narrow or
catalog snapshot; ready tokens use it to fence replacement without coupling to
unrelated shards. The [MCP lifecycle](MCP-CONTRACT.md#asynchronous-search-preparation)
owns durable queue and token semantics; source providers never choose reader policy.

## Coverage semantics

Conversation discovery returns whether the current-source catalog is complete and whether only active conversations are covered. A native `SessionTable` row becomes a conversation only when the same source ID has an observed `Msg_*` backing table；timestamped UI container rows without message storage are not reader-visible conversations. The account catalog, inbox, and participant discovery paginate after server-side policy filtering and bind cursors to reader/account/policy state. Inbox additionally freezes an observation sequence, keeps `indexed_conversations` fixed as the filtered current-epoch snapshot total across pages, reports `degraded_conversations` for exact conflict exclusions, and reports `catalog_fresh_as_of`, because it is the indexed activity surface rather than a synchronous full-source scan. Participant discovery binds its visible candidate set and returns roster completeness plus observed sender time bounds. Search reports indexed conversation/history bounds and unavailable shards separately from source/catalog completeness. A not-found response means “not observed within this coverage,” never global nonexistence.

## Stable source identity

- Account ID derives from the installation-private stable account binding via `source_account_key`, not filesystem location or a database encryption key.
- Principal-eligible keys are internal synthetic IDs only.
- The repository independently whitelists principal key kinds; provider booleans cannot promote a mutable handle into canonical identity.
- Conversation-local sender tokens are scoped keys and cannot link across conversations.
- `public_handle`, remark, nickname, group card, and surface display names are observations only.
- On the supported native build, direct statuses `0|1` are received and `2|3` are account-sent；other values stay unresolved rather than falling back to the peer. Outgoing rows resolve to the explicit self principal even if a group message has no sender prefix.
- System/recall rows are conversation events without human sender evidence and never match a participant speaker filter.
- Account self is created during account bootstrap even when no roster/outgoing row currently exposes it. Contradictory outgoing sender evidence fails closed.

## Admission semantics

In full local mode, source catalog/page reads and message parsing happen before the atomic `window.db` admission transaction. The still-open catalog snapshot or typed dependency session performs its final live validation inside the short transaction immediately before commit. Schema-v5 admission then records the message's semantic projection epoch and first/current observation sequence. Current-epoch `recent/context/message/range/speaker` reads and the native inbox normally select those materialized rows after the writer lock is released without opening a new source context; their receipts are explicitly `window_db`/bounded-stale. Ordinary materialized message assembly pins cursor checks, row selection, identity/resource projection and its receipt to one query-only SQLite read snapshot. Timeline progress and optional voice preparation run after it closes; progress remains bounded by the delivered snapshot's observation watermark. Updates retain their bounded delivery/ACK transaction, and full-local search still current-source-validates every returned candidate.

In remote core mode, source validation already completed at the edge's seal
boundary. `PreparedRemoteCapture` returns the envelope before terminal receive
ACK. Its local admission hook shares the reader's outer WindowDB transaction:
ingestion, an old reader delivery ACK, a new delivery/request outcome, the request
terminal claim and the transport ledger commit or roll back together. Only a
post-commit local ticket completion permits the broker thread to send the durable
wire ACK. There is no transport wait in the writer or source-session close. Fresh
source failure still rolls back reader ACK; transport loss recovery never advances
it. Exact terminal ACKs and stream high-water remain durable after body cleanup.

Cold resource read/search no longer rehydrates the owning message or opens two global snapshots. The already-authorized active canonical resource row owns one binding-authenticated source locator and a captured resolver revision. The service opens one `resource` session, reads that exact locator once through the provider's existing no-follow/link/digest/file-mutation checks, lets the session revalidate its selected auxiliary databases and returned file, and then closes the source lease. MIME sniffing, PDF/image/Office/archive processing and CAS staging happen afterward, outside both the source session and the `window.db` writer lock. A short transaction re-authorizes the row and compares the resolver revision before binding any original or derivative object; a race leaves the immutable CAS object unbound for normal cleanup. When the required source variant already has a private CAS binding, `wechat_read_resource` stays entirely local while retaining the same authorization, active-resolver, per-read object-integrity and revision checks. Ordinary long source scans, parsing, projection, receipt persistence, and processor work therefore do not monopolize the SQLite writer lock. Current-source responses select the exact stable message IDs in the admitted page; older retained observations remain auditable but are not silently presented under a newer receipt.

## Search and historical coverage

Synthetic M2 search and updates retain the explicit bounded full-conversation traversal used by the deterministic regression baseline. Full-local native account-wide search instead queries `window.db.search_text`, reports whether indexed coverage is complete, and current-source-validates the bounded candidates it may return. Historical indexing is performed by durable per-conversation/account backfill jobs with required message bounds, processed counts and resume cursors. Tail work is scheduled before backfill so old history cannot delay new-message observation. Pure shard generation replacement with byte-equivalent current canonical payload does not create a reader-visible observation; changed payload/state/parser evidence does. Idempotence compares the current episode only: A→B→A appends a new observation for the final A and points the current projection at that actual episode.

Search candidate recall is parameterized `window.db` matching over visible text and normalized link search fields; a casefolded FTS5 trigram index supplies only a necessary-condition accelerator and never evidence, and a query whose eligible terms are all shorter than three characters (or otherwise unsupported by the index) uses the bounded resumable timeline fallback. URL query/fragment are excluded. For a source-backed search, final hits are restricted to stable message IDs re-admitted from current source, so a retained historical row missing from the validated page cannot become a current search result. Ordinary remote replica search reports its bounded-stale local evidence instead. Absence still does not create deletion/recall evidence.

## M3 resource binding

Messages carry resource descriptors. The synthetic manifest registry maps a stable provider-owned `source_resource_key` to a relative local resource. The native provider instead creates an integrity-bound internal locator from stable message/conversation/month/name/hash evidence; it contains no absolute path. Neither provider returns its locator or source key through MCP. The repository derives the external resource ID from message ID plus stable source key; when a parser has no source key, declared digest/size/kind evidence supplies a stable fallback and ambiguous duplicate evidence fails closed. Parser discovery reorder therefore does not silently change resource identity.

Native files are searched only under `<account root>/msg/file/<YYYY-MM>/` by sanitized basename plus numbered duplicate variants. All candidates must be regular, single-link and content-equivalent; declared size and MD5/SHA-256 are checked. Native image resolution never treats the digest in message XML as a filename or payload-integrity claim. It first resolves a WeChat-assigned file hash through the optional encrypted `message/message_resource.db`, then the optional `hardlink/hardlink.db`, and otherwise may use only the exact message-positioned `cache/<YYYY-MM>/Message/<md5(conversation)>/Thumb/<local_id>_<create_time>_thumb.jpg` entry. A mapped entry that still needs an unenrolled V2 decoder key is never decoded from ciphertext: when that exact message-positioned preview is also locally present and decodable, the descriptor reports `preview_only` and serves that preview while the original stays explicitly unavailable; when no decodable preview is local, the mapped entry keeps its explicit `key_missing` state. Optional mapping DBs are admitted only when their page-1 keys verify and remain inside the same physical snapshot contract. In `msg/attach`, `<hash>_h.dat` and bare `<hash>.dat` are original-class entries; `_t.dat`, `_M.dat`, `_t_M.dat`, and the cache thumbnail are preview-class. `SourceResourcePayload.variant` distinguishes `original` from `thumbnail`: a preview-only descriptor can satisfy preview and never satisfies original or an unevidenced declared-size/hash claim. Non-file app messages, including Finder/Channels shares, expose a preview descriptor only when that exact cache entry is locally evidenced; an absent cache entry does not create a phantom resource. Native video probing is additionally confined to an observed digest-named `msg/video/<YYYY-MM>` layout. Directory traversal and final opens use `O_NOFOLLOW`; reads are bounded and compare descriptor/path identity before and after. Symlink, hardlink, ambiguous bytes, integrity mismatch and mutation fail closed. Successful bytes then enter the same MIME-sniff, cache and image/PDF/text/rich-file processing path as synthetic resources. Bounded processors cover DOCX/XLSX/PPTX (including per-slide speaker notes), JSON/XML/HTML, CSV/TSV and ZIP; Office active content/external relationships and unsafe/encrypted/archive-bomb members fail closed. No network/CDN fallback exists.

The synthetic V2 registry may carry an AES key solely to exercise the encrypted-image fixture. The provider verifies the encoded source object under the snapshot, decodes the V2 envelope in memory, and returns only decoded bounded bytes to the resource layer. Native V2 enrollment is an operator path separate from DB-key import: `sightglassctl source image-key import` accepts exactly 32 hexadecimal characters from an owner-private no-follow file or stdin while the daemon is stopped, stores only the account-binding-scoped secret in Sightglass Keychain, and never accepts literal key material in argv. Native provider construction retrieves and validates that Keychain value transiently; absent/malformed enrollment returns explicit `image_decoder_key_missing` (or, when a decodable message-positioned preview is also local, a `preview_only` descriptor) rather than encrypted bytes. Status is content-free, removal deletes only that binding's image key, and neither operation writes key material into config, `window.db`, stdout, receipts, tests, or Git. The V2 envelope's trailing XOR-protected bytes use a per-account tail constant. Earlier decoding used a hardcoded constant that corrupted the tail; the resolver now infers the correct constant from the known decoded footer and, when the footer is unknown (for example a WXGF/WebP payload that carries none), needs either an enrolled optional account XOR value (`--xor-key-file`, an owner-private two-hexadecimal-character file combined with the Keychain material) or one earlier successful known-footer decode in the same resolver; otherwise it fails closed with `image_xor_key_missing` rather than emitting corrupted bytes. A native image source binding carries a recorded decoder provenance (processor identity plus the captured resource revision); a provider/parser change that alters decoder behaviour invalidates every older unversioned native image object so its preview regenerates instead of reusing stale bytes.

Resource resolution never performs a network request. Missing, blocked, key-missing, unsupported, decode-failed, or too-large states are explicit resource errors; they are not interpreted as an empty successful attachment.

## Current limits

First-party key extraction, automatic key refresh/new-shard enrollment, sticker network retrieval, deeper forwarded nested-media byte recovery, and general audio/video transcoding are not implemented. Native V2 Keychain enrollment, mapped/cache-thumbnail preview recovery, bounded forwarded item projection, observed-layout local video probing, optional encrypted-media voice extraction, and local sticker-cache decode with in-memory account FileXorKey derivation and bundled-ffmpeg WXGF preview are source-complete; they remain distinct from whether this installation has the corresponding optional keys and from named-host playback acceptance. Background tailing and bounded resumable backfill are implemented, but indexed-history completeness is reported per current durable state rather than assumed. Native local file/image/video/voice recovery and rich-file processing are source-tested; installation-specific live byte evidence belongs in private operator records, outside Git. Synthetic SQLite committed WAL continues to prove the complete fixture contract. Native message reads use installed `sqlcipher3`/SQLCipher and `zstandard`, but do not import the WGO runtime; optional WGO CAS/knowledge adapters remain separate future work.

Cold link/retrieval discovery uses `scan_discovery_page` with a provider-private
physical position and a maximum of 256 inspected raw rows per call. Time filtering
does not turn into an unbounded source sort; empty filtered pages still progress.
Physical order does not assert chronology, and exact-boundary completion may require
one final empty page. Rows are candidates: the reader reconciles matching identities
through canonical `get_message`, rechecks hard scope, and admits only validated hits
plus bounded context. Private positions are reusable only under the same selected
logical shard generations. Neither provider changes source indexes or writes to the
source; native context retains its separately documented bounded-memory scan behavior.

## Storage admission boundary

Provider evidence does not choose retention or storage policy. Reader admission checks the daemon-owned storage budget before opening a source operation, reserves prepared batch growth and validates the snapshot before commit. A rejected batch does not advance durable source/backfill positions. Soft pressure pauses historical jobs; hard/free-space pressure also stops foreground hydrate and tail. Synthetic snapshot copies and resource processor staging reserve private owned-volume space; native handles remain read-only SQLCipher and create no decrypted copy. Source observations are encoded only in the model's lossless codec after source interpretation; raw digest identity and search/current-message projections are unchanged. See [storage operations](OPERATIONS.md#storage-budget-and-maintenance).

Schema v10 residency belongs to the reader, not the provider. On-demand idle collection
skips body sessions; keep is prospective and explicit historical jobs are keep-only.
Recent admission enforces the selected time/byte window. Foreground search can scan a
bounded source page but admits only canonical literal/speaker candidates under temporary
residency; nonmatches do not become cached bodies or contiguous source coverage. Each
retained search hit records only its exact focus window. Explicit context reads acquire
necessary neighbors separately. Catalog readiness may settle on-demand conversations
without asserting a tail/frontier or complete history; native inbox projects valid
resident observations only. Expiry preserves observed positions and cannot cause refill.

## Foreground admission and read-plane evidence

A current-epoch present message observation establishes local message/context
readability independently of the background conversation tail. This partial evidence
never marks a catalog conversation handled or a historical backfill complete. An
unfiltered source page from a cursor-free native incremental recent read does observe
a real contiguous window: a new conversation/current projection epoch may seed its
frontier in the same validated admission. Existing unverified legacy coverage instead
uses bounded forward revalidation. A later recent window does not advance an existing
frontier across an unseen interval. Schema v9 stores validated, disjoint source read
windows plus a contiguous sync frontier, a contiguous history floor and independent
forward/history completion evidence. A recent refresh wholly within a proven frontier
preserves established full-history completion; an unproven flag or new unseen forward
gap remains partial. Projection cropping and sender filters do not
choose those positions. Source validation failure rolls back rows and evidence together.

Background sync continues after the contiguous frontier, so observing 181–200 after
1–100 still leaves 101–180 for bounded subsequent sync. Historical backfill continues
before the contiguous floor, rather than before the earliest independently observed
island. Overlapping windows merge only with validated page/anchor evidence. A legacy
v8 tail/complete flag supplies no continuity proof: metadata-only v9 migration preserves
messages and observations, while bounded forward sync revalidates from the oldest
source page and persists each successful step for restart. An unchanged physical
source poll cannot skip unverified coverage or unfinished forward recovery. There is
no startup full-history scan and no automatic observation-history rewrite.
A valid admitted anchor resolves the selected account/conversation before opening its
source session; current source existence/sort, policy and dependency checks still apply.
The reader's explicit `refresh=true` option bypasses local projection selection for
one bounded page and reuses this same source/admission path. It does not add a provider
method or trigger full-history indexing; cursor continuations and updates cannot use it.

Local ordinary context and speaker context neighbors are bounded to the validated
window containing each focus row. A disconnected admitted island is not an adjacent
source message; an unverified legacy focus therefore contributes no guessed neighbors.
The materialized receipt reports `continuity.state` as `unverified`,
`disjoint_windows`, or `validated_window`, plus `validated_window_count` and
`context_neighbors="same_validated_window_only"`. Explicit bounded refresh can recover
the missing neighborhood without triggering full history.

Speaker literal filtering examines at most 10,001 candidates per page. When that
bounded scan has no visible hit, or `system_policy="omit"` hides a whole page, a signed
continuation uses the last scanned candidate. The boundary is admitted for cursor
reconciliation but is not part of that page's delivered rows and cannot seed
update progress, count as reader progress, or ACK a delivery. Query/sender/time/system-policy scope and cursor version remain bound.

Operator observation maintenance uses bounded read-only inspection and an explicitly
requested restart-safe repair batch. It preserves immutable observations and legitimate
identity correction bindings. Matching historical body/source evidence can support a
new repair episode; unavailable evidence invalidates local readability until normal
source reobservation. Repair revisions and derived-index versions stale affected local
traversals, while already-spooled pending deliveries remain immutable. See
[storage and observation maintenance](OPERATIONS.md#storage-budget-and-maintenance).
