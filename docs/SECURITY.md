# Security and privacy boundary

M0–M4 remains the deterministic synthetic assurance baseline. Native source access requires explicit operator authorization for one configured local macOS WeChat account. A fresh initialization starts from a selected one-conversation allowlist; account-wide `all_except_denylist` needs the account owner's separate privacy decision. Account activation does not authorize another account or remote content egress. Sightglass adds no WeChat write, injection, app re-signing, implicit URL/resource fetch, public listener, or direct ChatGPT filesystem access.

The thin Mac edge / Linux core implementation is a source candidate. Production
migration and real remote content egress have not been activated. Native
interpretation remains implementation v6 / parser v2 / WindowDB schema v10. An
official Linux WeChat native source adapter (P6) is not implemented; the
`remote-capture` core binding uses a declared Mac or synthetic origin. Linux
resource/ASR source and actual target acceptance are separate facts, recorded in
[Linux processing](LINUX-PROCESSING.md) and [current state](current-state.md).

Controls present now:

- strict/fail-closed source-backed reads plus a separately identified current-epoch materialized read plane; local projection/CAS responses never claim a new live-source confirmation;
- config v2 provider registry with explicit `synthetic` / `macos-wechat` / `remote-capture` binding; the last is an offline core assembly, not a Linux native source. Source schema v10 adds independent residency/settings/lease/body-availability state and contentless-delete lexical derivatives to the v9 coverage/maintenance foundation. Normal startup refuses older schemas before DDL; v9→v10 conversion is explicit, stopped-only, bounded and resumable. One frozen committed input produces both exact verified compressed recovery and candidate, with raw observation bytes preserved except for separately approved cache release. Rowid/identity mappings, sequence high-water marks and durable rows are compared before publication; historical migration helpers remain fixture-only;
- exact native app/source candidate and profile binding for WeChat `4.1.13 / 269602 / arm64`, with opaque discovery output and no automatic account selection across candidates;
- bounded key-map import through a no-follow owner-held regular single-link descriptor with pre/post identity verification, or in-process reuse of the same binding's installed Keychain map; both paths freshly verify page-1 HMAC for every current contact/session/message DB before a complete set is admitted;
- native DB keys stored in a stable account-binding-scoped Sightglass Keychain item, loaded in-process, and excluded from subprocess argv, config, logs, receipts, tests, and MCP responses; database credentials do not define account identity;
- native SQLCipher URI `mode=ro` plus `query_only=ON`; no decrypted database cache/copy is created;
- logical native dependency generations separated from physical in-flight revision: a live timeline cursor binds only the message shards that actually served its conversation, so ordinary append preserves the cursor while selected-shard replacement stales it without inheriting unrelated account generations. A catalog snapshot checks every admitted encrypted main/WAL device/inode/size/mtime and page-1 identity; a typed conversation/message/resource session pins one read transaction per opened database and rechecks its recorded database/file read set, eligible message-shard routing and negative facts, and the exact profile binding. Materialized timeline cursors instead bind reader/account/conversation/filter, policy revision, append-only identity-correction ledger revision, semantic projection epoch and observation watermark, exclude later appends, and stale if a previously visible row or identity projection is corrected;
- full-manifest/content/WAL-bound private SQLite snapshot copies, plus stopped-only adjacent compressed snapshots and exact-plan acknowledgements for destructive restore/retirement. Backup commands share the daemon process lock, reject symlink/non-private/multi-link files, fsync atomic outputs, and never place source content or digests in MCP;
- slow source discovery/page reads and message parsing outside the foreground SQLite writer transaction, followed by short atomic `window.db` admission committed only after final catalog-snapshot or selected-dependency validation; current-epoch `recent/context/message/range/speaker` pages and the warm native inbox may then project from `window.db` without a provider context, while updates retain their bounded delivery/ACK transaction and search still validates candidates against the current source;
- independent Event notifications occur after the outer queue-admission commit and writer release; durable leases/fencing plus bounded fallback remain authoritative when a hint is lost. Worker error status contains exception class and public module/function/line only, separates current from bounded historical failures, and never renders exception messages, arguments, notes, traceback paths or frames;
- durable account/catalog/shard/conversation/backfill state, bounded round-robin live-tail polling, tail-first scheduling, pause/resume, and resumable historical backfill;
- source-side keyset pagination, lazy directional multi-shard merge, stable server/local message identity, and conflicting duplicate reconciliation before page limits;
- server-side reader identity and conversation policy;
- opaque external IDs and signed message anchors;
- random per-install mode-`0600` HMAC secret; signed reader/account/scope-bound timeline and search cursors with expiry, using current-source reconciliation for live/search cursors and projection-epoch/observation-watermark reconciliation for materialized timelines;
- independent conversation/participant/filter-set timeline and update state;
- process-wide owned-storage accounting covers DB/WAL, spool, resource/voice/processor staging and local backups; concurrent reservations and a filesystem free floor apply before admission, with a bounded maintenance allowance for ACK/cleanup/recovery. This is backpressure, not a filesystem quota. Separate recent/on-demand lifecycles bound disposable bodies; existing stock stays protected until exact release preview/apply;
- one current-body view governs literal/card/link/semantic/consistency inputs. Exact duplicate fields are normalized on admission or in explicit stopped-only compact; existing v10 stock is read-compatible without startup rewrite. A conflicting legacy embedded body fails compact construction before publication. Body release is logical cache retirement, not secure erasure of backups, SQLite pages or previously delivered client data; pending replay and active input dependencies protect their body until the existing lifecycle permits release;
- every admission consumes the independent keep/recent/on-demand decision. Expiry preserves identity evidence, corrections, observed positions and exact pending replay, pins only active resource/voice/delivery dependencies, and cannot create a source deletion, reader update or automatic refill. Current bodies under bounded release jobs and expired temporary bodies are excluded from ordinary local candidates before physical cleanup finishes;
- schema/backend activation selects runtime/config/DB/private state with one atomic selector and durable journal. Candidate staging copies its spool/CAS/token-secret and known sidecars into an independent owner-private namespace, verifies bytes/digests and rewrites only internal storage paths. New ACK/cleanup cannot destroy the preserved old pair. Source/dependency provenance, schema/backend, unchanged original DB, exact recovery, workspace budget and physical free floor are checked. Native recovery/staging requires verified encrypted volumes; old DB/pairs are never retired to force headroom;
- read-only storage explanation is operator-authenticated and never MCP-visible. File inventory returns only owned root-relative names and metadata without following symlinks or reading file content; SQLite inspection returns schema-level sizes/counts plus aggregate byte counts from a bounded in-memory legacy-payload sample, never stored content, configured absolute roots, or automatic cleanup/retention actions;
- the daemon keeps at most 64 content-free UTC-day storage snapshots in a separate mode-`0600`, single-link, no-follow, atomically replaced sidecar. Entries contain fixed storage byte components, message/observation counts and timestamps only—no message text, labels, identifiers, filenames, paths or credentials. `storage explain` reads but never writes this sidecar; malformed or unsafe history fails closed as unavailable without blocking ordinary reads;
- new observation payloads use a versioned lossless BLOB codec with length/CRC/complete-stream checks; legacy TEXT remains readable and no bulk startup rewrite occurs. Raw envelope and sender-key data remain private regardless of compression;
- mode-`0700` delivery spool with immutable mode-`0600` exact response payloads, one pending delivery per scope, idempotent ACK, and ACK rollback on source failure; a storage-only admission failure may commit an already validated ACK within the maintenance reserve and reports `ack_committed=true`;
- policy changes expire pending deliveries and invalidate policy-bound account cursors; allowlist/denylist is enforced for catalog, inbox, discovery, direct IDs, context, updates, search, and resource reads;
- parameterized local search candidate recall; full-local source reads and remote fresh verification return only canonically confirmed IDs, while ordinary replica reads report bounded-stale local evidence;
- ordinary asynchronous success/failure access receipts contain no natural-language body, label, filename, URL, or path; the nonblocking queue is capped at 4096, graceful shutdown drains queued rows, and persistence blockage/failure or saturation is observable without overwriting an otherwise successful reader result. Reserved internal capture/request/recovery rows are private durable protocol state, separate from this audit queue and its retention; natural-language queries never enter them;
- no raw principal IDs or source paths in MCP results; ordinary public link projection strips raw URL, credentials, port, query, and fragment while search indexes only bounded title/description/source/normalized host/path fields. The sole URL-egress exception is the owner-authorized `wechat_find_links`/`wechat_retrieve` discovery results, which may project the full observed raw/normalized URL (including credentials, port, query, fragment); no log, access receipt or public fixture ever contains a real URL, and no tool fetches a URL;
- decoded anchors contain only opaque external IDs; non-text transport XML and absolute resource paths are not projected;
- private `window.db` directory/file modes;
- message-bound resource IDs with owning-conversation authorization and active-resolver checks repeated before catalog metadata, preview/text, original, or search egress; materialized resource finder cursors bind reader/account/filter/policy and an observation watermark, and neither cursor nor access receipt stores query text or filename. Warm CAS reads re-verify digest/size/path safety on every use and compare the captured resolver revision before committing any derivative binding; cold misses acquire the exact locator once inside a resource-scoped lease, preserve all database/file integrity checks, release source handles before processor work, and repeat the resolver-revision comparison during short binding admission;
- provider-internal resource locators contain no absolute path and never cross MCP; native locator integrity binds stable message/conversation/month/name/hash metadata to the configured source account;
- descriptor-relative directory-FD `O_NOFOLLOW` traversal; path escape, parent/final symlink, non-regular file, and multi-link inode fail closed;
- bounded no-follow reads, pre/post descriptor and directory-entry identity checks, equivalent-only numbered duplicate reconciliation, and declared MD5/SHA-256/size verification;
- V2 image AES decode in memory; native image keys are account-binding-scoped Sightglass Keychain secrets enrolled only from an owner-private no-follow file or stdin while the daemon is stopped. Literal key argv is rejected, and key material never enters config, `window.db`, stdout, subprocess argv, cache metadata, receipts, tests, Git, or MCP responses;
- source-original and source-kept thumbnail variants are distinct: `preview_only` may satisfy preview, but its bytes cannot satisfy original or the original's declared digest/size; cached objects remain variant-bound;
- MIME sniff before processing, including HEIC/TIFF/BMP and bounded audio/video inspection; declared MIME mismatch is reported, while binary-as-text, malformed media, encrypted PDFs, Office active content/external relationships, unsafe/encrypted ZIP members, archive bombs, and integrity mismatch fail closed. UTF-8/UTF-16/GB18030 decode is strict, and supported video preview extracts only one bounded local frame without network retrieval or general transcoding;
- image header checks before processor launch, then decoded dimensions/pixels validation; EXIF is not projected and previews are re-encoded PNG;
- external PDF/image processors receive private temporary files, no stdin, bounded outputs, suppressed untrusted stderr, and a 20-second timeout. macOS image processing uses `sips`; the Linux candidate uses system libvips through `vipsheader` / `vipsthumbnail`. In-process rich parsers bound archive members/bytes/ratio/name length, XML/JSON nodes/depth, table rows/cells and XLSX cell ranges;
- local voice transcription adds no implicit network request or source write. The macOS pipeline runs in three isolated child processes per job, all started with `start_new_session=True` so the daemon can terminate the whole process group:
  - the SILK→PCM decode child (`python -m sightglass.voice._decode_child`) receives input and output only as inherited descriptors, never as argv data, and is bounded by input bytes, `RLIMIT_FSIZE`, an explicit decoded-size check, a zero-byte failure rule, and a wall-clock timeout; only the SILK V3 envelope is accepted and everything else fails closed;
  - the precompiled Swift helper receives one private staging PCM path and an explicit locale, and is bounded by a 2 MiB stdout cap, a 64 KiB stderr cap, and the configured wall-clock timeout; it opens no microphone and downloads no model;
  - a helper that exceeds its wall clock is killed by process group, not merely abandoned, and both parent and child output buffers are hard-capped rather than unbounded;
- voice staging lives under mode-`0700` `voice-work/`, files are created `O_NOFOLLOW|O_CREAT|O_TRUNC` at mode `0600`, are fsynced, and are deleted as soon as one job finishes; a startup sweep removes non-recursive leftovers older than one hour;
- raw SILK/PCM bytes, staging paths, and transcript text never enter Git; transcript text is derived data and is stored only in the private content-addressed store, and the recorded provenance is content-free (recipe/decoder/envelope/PCM spec, resource revision, digest, byte/frame/duration counts, recognizer locale/asset/model/OS identity) with no paths or key material;
- a helper subprocess receives only an allowlisted environment (`HOME/LANG/LC_ALL/PATH/TMPDIR/USER`) with any KEY/TOKEN/SECRET/PASSWORD/CREDENTIAL-shaped name removed;
- mode-`0700` private resource cache with SHA-256 names, mode-`0600` single-link objects, atomic publish, and digest/size verification on every binding read;
- one long-running `sightglassd` owns each core's `window.db`, reader delivery spool, processors, cache and durable resource jobs; in full local mode it also owns native source access. Remote mode places native sessions and the capture spool on the thin Mac edge. `sightglass-mcp` owns only MCP-to-IPC projection. Under the same runtime read gate, the daemon classifies exact calls as local-only or source-required; local-only execution cannot silently fall back to the provider if its premise changes and does not cancel/wait for `SourceWorker`;
- daemon summary status uses out-of-band cached reader health and small published derivative metadata, without source access or historical counts. Local read, source read, resource derivation, the database writer and transcript wait have independent bounded capacity; equivalent acquisition/derivation stages single-flight. Long derivations persist an exact validated recipe with bounded retry, lease and fencing state, and the worker re-verifies its current fence inside the same writer transaction that publishes a binding, so an expired or taken-over worker cannot publish. Status exposes only content-free build/config identity, lane/job/operation counters, bounded latency outcomes, failure classes and writer/worker diagnostics. Explicit operator-only `daemon.status(operations_only=true, include_stack=true)` samples at most eight public `sightglass.*` module/function/line locations from the oldest active operation, without database access or retaining frames; locals, arguments, SQL, file paths, thread IDs and content are excluded. Reader/MCP status never samples or exposes stacks;
- mode-`0600` Unix socket inside a mode-`0700` runtime directory, effective peer-UID verification, bounded length-prefixed JSON frames, and no TCP/public listener;
- distinct random reader/operator credentials in the platform secret store: macOS Keychain or the Linux owner-private, no-follow file store. Config/reader profiles retain only SHA-256 hashes, authentication uses constant-time matching, and startup checks that stored credentials match config. Native DB/image keys remain in the Mac edge's account-bound Keychain;
- role-separated IPC allowlists: reader can call only daemon status and the exact thirteen MCP tools; pause/scope/backfill/cache/alias/correction/shutdown IPC operations require the operator token and are never MCP tools. Backup lifecycle is likewise absent from MCP and is allowed only while the daemon is stopped and the CLI owns the runtime process lock;
- mode-`0600` atomic config, process-identity lock, DB, immutable delivery files, cache objects, and daemon log; mode-`0700` config/data/socket/spool/cache directories;
- singleton process lock and stale-socket replacement only after lock acquisition; daemon restart changes process identity while retaining durable pending delivery payloads;
- cache cleanup previews by default; apply also continues bounded temporary-body expiry and explicitly approved release jobs. CAS unlink remains bounded to DB-unbound, regular, single-link, digest-named private objects under the writer fence; pending replay and active resource/voice jobs stay protected;
- append-only operator correction ledger for local alias, merge/split, source-key rebind and rollback; corrections use the original observation’s retained sender-key evidence after body release. Observation identities/episodes/raw digests and already-materialized pending payloads remain durable; only approved cache lifecycles may replace disposable observation bodies with versioned retained headers;
- `doctor` reports file-mode/Keychain/source-settings readiness, selected provider mode, and volume encryption as separate facts;
- no implicit URL/resource fetch or WeChat mutation path; the optional BGE-M3/Vectorize lane is disabled by default, requires exact source account/conversations plus explicit external-data consent, and re-admits every remote candidate against current canonical observation, epoch and policy. Credentials use the platform secret store and never subprocess arguments. Pending publication is readback-only after ambiguous submission; a rebuild rotates namespace without deleting remote rows. The explicit Cloudflare benchmark sends only its fixed generated synthetic corpus and vectors, uses an isolated experiment index, and never reads a configured account. The v2 encoder deduplicates identical canonical fields, skips empty/unknown-placeholder inputs and fences old recipes; non-text neighbors retain their ordinary authorization and resource boundary.
- MCP stdout reserved for protocol;
- committed fixtures are builders, not personal databases;
- content remains ordinary structured data.

Current hard resource ceilings include 32 MiB decoded source bytes, 8 MiB reader binary egress per call, 40,000,000 image pixels, 16,384 pixels per image dimension, 500 PDF pages, 4 MiB extracted PDF text, 500 requested text lines, and 20 seconds per external processor. ZIP processing additionally caps 2,048 members, 64 MiB total uncompressed data, 16 MiB per member, 1,000:1 compression ratio and 1,024 UTF-8 filename bytes; structured parsers cap 100,000 nodes and depth 64; tables cap 50,000 rows/250,000 cells and an XLSX selected range caps 10,000 cells. The ordinary MCP default egress request is 4 MiB. Voice additionally caps 4 MiB per captured SILK original, 300 seconds / 9,600,000 bytes of decoded PCM, 60 seconds of decode wall clock, 120 seconds of helper wall clock (configurable 1–600), 200,000 transcript characters, and 2 MiB of helper stdout. A duration claimed by the source envelope never relaxes any of these.

Controls still reserved for later work include first-party key extraction/refresh, new-shard enrollment without an explicit import, sticker network retrieval, deeper forwarded nested-media byte recovery, general audio/video transcoding, and optional WGO adapters. Native V2 Keychain enrollment, mapping-DB/cache-thumbnail preview recovery, bounded forwarded item projection, observed-layout local video probing, local sticker-cache decode, exact `VoiceInfo.voice_data` SILK extraction from an enrolled/page-1-verified `message/media_*.db`, and the local SILK→PCM→Apple `SpeechAnalyzer` transcription pipeline are present source capabilities. The sticker FileXorKey is derived in process memory from the current account's own local state behind an exact profile/build gate, validated against local ciphertext prefixes, and is never persisted, enrolled, or emitted. Voice correlation requires an exact `Name2Id.user_name → rowid` plus `chat_name_id/local_id/svr_id/create_time` match and fails closed on missing/ambiguous evidence. Optional database/image keys, decoder/helper readiness and language speech assets must be checked for each installation. Local recognition, resource egress and remote-client consumption are separate verification states. Installation receipts and account details remain outside Git.

The deterministic signing and V2 image keys exist only in test fixtures. The runtime signing secret is installation-local and never returned by MCP. Delivery payload references, resource bindings, object paths, and local spool/cache paths remain internal `window.db` state.

Source support, installed code, account-scope authorization, active policy, indexed coverage, local stdio, tunnel transport and named-host acceptance are separate facts. Account scope authorizes only conversations discoverable in the configured account and still excludes the denylist. Catalog/roster/history/resource coverage remains explicit: metadata-only, missing, unsupported and key-missing states mean callers must not infer global absence or original attachment availability from an empty result. `docs/current-state.md` records which live payload variants and host surfaces have actually passed content-free acceptance.

## Capture, ownership and transfer

The [sealed capture protocol](CAPTURE-PROTOCOL.md) admits only one complete typed
bounded operation per task. The independent edge ceiling binds one exact account
and conversation set and filters catalog before serialization. Native source and
decoder keys remain on the Mac. The wire carries no arbitrary provider methods,
SQL, shell or path commands. A native narrow message session fences eligible
shard membership and negative target-table facts; changed unopened negatives get
a fresh revision-validated read-only check. Cached contact metadata also records
its selected database and must match cache provenance. Each operation has a finite
read-set fence; a sequence of batches does not promise one global mutation
snapshot. Bounded absence never means deletion or recall.

Four capabilities remain separate:

| Capability | Bound authority |
| --- | --- |
| Reader | Current ReaderPolicy and principal state for MCP/reader results and exact local replay. |
| Operator | Private daemon control and explicitly authorized stopped-only recovery/transfer procedures. |
| Edge | Origin-bound capture frames over the restricted SSH command, a distinct edge token and the enrolled core generation. |
| Core | Active local writer ownership for one host, WindowDB namespace and generation, with its outside-export credential. |

The broker uses an owner-private Unix socket and kernel peer UID checks. The edge
originates SSH with strict known-host verification and one fixed remote command;
there is no public capture listener. `hello` and `ready` bind the exact
`core_generation`. Core and edge ownership callbacks recheck active grants at
capture, spool publication, transfer and terminal release boundaries. Reader or
operator IPC credentials do not authenticate an edge connection.

Activation schema `sightglass.activation.v2` binds role, host identity, actual
WindowDB/spool namespace, generation, predecessor and monotonic ownership counter
to a separate private writer credential outside the exported data namespace.
A revoked generation cannot be reactivated; a successor requires an explicit
owner transition. A copied process lock or activation file is not distributed
fencing, and an older installed wheel may not understand this guard. Before
cutover, the operator must actually stop and revoke the previous supervisor,
tunnel and credentials. A preserved recovery copy receives no writer credential;
rollback needs a new owner generation and the latest authoritative reader state.

Fresh capture admission shares one WindowDB transaction with bodies, an old
reader ACK, the new delivery/request outcome and the transport ledger. The exact
request terminal claim cannot overwrite a prior accepted/rejected winner. Source
failure rolls reader ACK back; only the existing validated storage-pressure
exception can commit it under the maintenance reserve. Post-commit ticket
completion permits wire ACK. Exact transport terminal receipts and stream
high-water are permanently retained outside ordinary receipt GC. Durable reader
request replay within its 30-day horizon still rechecks current policy, principal
revocation and pause state.

Stopped-only capture recovery preserves an existing immutable terminal ACK; only
an exact authorized unreceived batch may receive a new `epoch_loss` decision.
The private plan, terminal receipt and monotonic new-epoch floor bind recovery;
reader ACK and source coverage do not advance. Frozen installation transfer
verifies a logical cut across receive high-water, reader state/request outcomes
and the exact owned delivery spool/CAS files. A remote cut requires a stopped edge
export and prior resolution of any unreceived pending batch. An initial full local
installation has no edge export. Neither byte-valid transfer nor recovery
activation grants source-content egress permission. See the
[operator procedures](OPERATIONS.md) for preconditions and recovery commands.

## Synthetic Linux verification

Portable CI runs generated-source daemon/bridge checks on Linux. Unix peer identity comes from kernel `SO_PEERCRED` there and `getpeereid` on macOS; both retain the same-owner UID and separate token checks. Outside macOS, doctor reports volume encryption as unknown, and absent image processors return their explicit typed availability error. Linux verification does not enable a native WeChat provider or expand account authorization. The credential layout follows the [Linux Unix-socket contract](https://man7.org/linux/man-pages/man7/unix.7.html).

## Search preparation state

Full-local daemon search preparation uses a bounded owner-private atomic sidecar beside
`window.db` (mode 0600, no-follow load, owner/single-link checks, 32 jobs / 256 KiB).
It retains request scope and a query/sender digest, never query text or result hits.
The signed 15-minute token binds reader/policy/local store/source configuration/
projection epoch/request intent; ready delivery additionally checks selected logical
shard generations. Pure invalid-token/scope errors and pending polls stay on the local
lane; a pending-to-ready race cannot touch source under a local-only premise.
Pause durably fails old jobs/tokens. After restart, an identical token poll supplies
the query/speaker inputs in memory before interrupted conversations are re-scanned;
partial source proof is never reused. Terminal states are visible only after
atomic durable publication; unavailable metadata persistence remains explicitly
preparing. All source reads stay read-only, narrow main/WAL validation still precedes
admission commit, and final search still verifies current canonical IDs and filters.
No new network egress or source account permission is introduced.
