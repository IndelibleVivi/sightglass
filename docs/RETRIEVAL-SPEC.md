# Retrieval and resource fidelity extension

Revision: accepted retrieval extension, 2026-10-01 (`8650328`); selective-residency and offline-slimming extension accepted 2026-10-04 (`002d3e9` baseline).

This document records the product outcomes selected for the next development stage.
It supplements [SPEC.md](SPEC.md); the [complete ledger](IMPLEMENTATION-PLAN.md)
owns implementation and acceptance status. Source development does not authorize
installed migration, account access, model downloads, activation, deployment, history
rewrites, release tags or publication. Private dogfood content stays outside Git.

## Responsibilities and invariants

- `read_messages` reads a known timeline/anchor; `search_messages` preserves strict
  casefold/literal AND semantics and current-source validation; `retrieve` finds
  candidate contexts from concept, structured constraints and approximate hints.
- Canonical observed messages and resource bindings own facts. Link, lexical,
  topology and optional semantic representations are rebuildable derivatives.
- Link extraction uses only locally observed payloads. No URL fetch, redirects,
  Open Graph, favicon or implicit remote encoder is permitted.
- Derived publication fences the exact captured message/resource version in the
  same short transaction as rows/checkpoint. A failed or interrupted batch must
  not advance its checkpoint. Storage pressure can suspend rebuildable work.
- Discovery and retrieval never commit timeline positions, seed update cursors,
  ACK deliveries, schedule speech or generate attachment previews.
- Receipts separate source/history coverage, index coverage, scan termination,
  result continuation and materialized/live freshness. Zero results do not
  prove absence outside the observed scope.
- Cursors bind reader/account/query/policy/identity revision, projection epoch,
  observation watermark, applicable index generation/recipe and sort boundary.
  Later appends are excluded; repairs/backfill within a published view stale it.
- Diagnostics are content-free by default. Query text, URLs, source paths,
  account labels and encoder input never enter logs or access receipts.
- Owner decision (2026-10-01): `find_links` and `retrieve` may return the full
  observed raw/normalized URL, including credentials/query/fragment/port. This is an explicit
  egress exception for these authorized discovery results. Ordinary message link
  projection remains redacted; logs, access receipts and public fixtures never
  contain real URLs. Policy still governs every link's owning message.

## SG-051: Evidence and acceptance baseline

Synthetic aggregate-project fixtures must include over 1,200 distractors,
targets beyond the old 1,000-candidate budget, near-domain hints, adjacent links,
interleaved unrelated messages, duplicate URLs, a single target and separate
conversations. Exact domain, approximate hint and concept-only queries are distinct
acceptance cases. Task cost and individual call latency are distinct measurements.
Private image traces compare a failed image, a normal image and, when available,
a thumbnail that later acquires an original. Trace boundaries are selected native
candidate, decode output, processor input, generated preview, CAS binding and MCP
image content. Synthetic checks include truncated images and black/transparent
images; no quality score substitutes for locating the actual first failure.

## SG-052: Resource fidelity and recovery gates

Preview provenance binds captured resolver revision, source digest/variant,
processor/version and parameters. Original upgrade or decoder/recipe change must
invalidate incompatible cached previews. Resolver change between acquisition and
admission fails with `SOURCE_GENERATION_CHANGED`; captured A bytes cannot publish
as current B. The earlier gray-block image was traced to the account image
XOR-tail constant (previously hardcoded): the resolver now infers the constant
from the known decoded footer and, when no footer is known, needs an enrolled
optional account XOR value or one earlier successful known-footer decode in the
same resolver, else it fails closed. A separately authorized private trace
reproduced 21/21 identical rendered pixels through the canonical resource/CAS/MCP
chain; a real thumbnail→original upgrade on a live account remains unverified,
and no installed daemon activation or ChatGPT-host acceptance is claimed.

Recovery gates cover incomplete compressed-backup publication before schema
activation, restore namespace replacement before WAL/SHM retirement, reload
prepare/start/probe/swap/rollback, failed foreground-claim rollback, resource-job
attempt ceilings during crash recovery, and strict policy collection validation.
These are separate from implementing a source migration against synthetic stores.

## SG-053: Versioned links

`message_links` retains raw/normalized URLs privately, source path/ordinal,
host/path/query/fragment, locally supplied title/description, extraction recipe,
message observation version and digest. Extract text URLs, link cards and explicit
forwarded-item URLs without attributing inner display names to canonical actors.
Normalize outer punctuation, scheme/host case, IDNA and default ports; preserve
query/fragment and tracking parameters in canonical private values.

New admission/correction replaces links in its writer transaction. Historical
backfill computes outside the writer, fences captured versions, and publishes rows
and durable checkpoint atomically. Drift requeues rather than overwriting current
data. Recipe changes/rebuilds publish a new generation.

`wechat_find_links(query="", account_id=None, conversation_ids=None, domains=None,
hints=None, after=None, before=None, cursor=None, reading_token=None, limit=None,
response_profile="brief")` distinguishes exact
normalized-hostname constraints (apex and `www` are distinct) from bounded fuzzy hostname recall. Results carry message,
conversation, sender, source path, context anchor, full observed URL under the owner-authorized egress exception,
and explicit receipts.
The 2026-10-05 MCP default is 20 results for a new request. Omitted preparation
continuations recover the original job limit; diagnostic receipts remain explicit.
Revoked, stale and replaced links must not remain current-visible.

## SG-054: Search truth and lexical compatibility

Apply account/conversation/time/current-state predicates before candidate limits.
Sender predicates may be pushed down only when identity correction semantics
cannot cause false negatives; final canonical filtering always remains.
Expose execution stop reason, candidate count/budget and continuation independently
from source/history coverage. Source unavailability cannot claim live validation.

Any new lexical backend must reproduce the strict reference message-ID set for
English infixes/hyphens/domains/paths, Chinese one/two/long-character terms,
multi-part/cross-field queries and Unicode casefold. Unsupported indexed queries
use a bounded, resumable fallback. A 100k synthetic comparison measures storage,
build/rebuild, common/rare/absent queries, short Chinese terms, WAL, crash/restart,
drop/rebuild and latency before account-wide lexical backfill is enabled.

## SG-055: Deterministic candidate-context retrieval

`wechat_retrieve(concept, hints=None, account_id=None, conversation_ids=None,
participant_ids=None, kinds=None, count_hint=None, after=None, before=None,
cursor=None, reading_token=None, limit=None, response_profile="brief")` accepts message/link/image/file/voice kind constraints.
The 2026-10-05 MCP default is 3 contexts for a new request, with the versioned brief
projection and soft JSON targets defined by [MCP-CONTRACT.md](MCP-CONTRACT.md).
Diagnostic output retains full receipts within the same hard reader limits;
preparation continuations recover the original job limit when omitted.
Scope/sender/time/kinds are hard constraints; hints drive approximate recall;
count_hint is a soft ranking preference and never manufactures targets.

Structured and strict lexical lanes feed bounded same-conversation neighboring
and reply expansion. Recipe `sightglass.retrieval.rrf-context.v3` supplements the
±8-message body radius with link neighbors within 180 seconds, so dense unrelated
traffic cannot hide a nearby second link solely by row distance. Explicit native
reply server IDs use the canonical native token builder; only admitted targets
within the hard conversation/sender/time scope enter a context. Contexts are capped
at 32 messages and project at most 32 links with an explicit continuation guide.
Kind constraints select focus evidence; neighbor messages can carry another kind.
Overlapping windows may merge; time proximity is not a topic
claim. Transparent rank fusion and versioned topology evidence distinguish exact,
normalized/fuzzy hints, lexical support, semantic recall, link cooccurrence, reply
and sender continuity. A context takes its best semantic reciprocal
rank once, so many weak ANN hits do not reward a long unrelated discussion.
Link cooccurrence, count preference and explicit replies break primary-score ties;
they cannot override a stronger semantic/literal score. Results label focus versus
context-only messages and deduplicate project
URLs for count scoring while preserving distinct message evidence. Return canonical
compact messages, links/resources and replayable anchors, never generated summaries.
Exact and approximate aggregate-project benchmarks must hit the first page, avoid
cross-conversation merge, avoid duplicate targets/count padding, and preserve reader
state. Pure concepts with no lexical/structured overlap are semantic residual cases.

## SG-056: Semantic experiment and conditional lane

Compare message, local-window and topology-context units on Chinese synonyms,
mixed-language names, pronouns, bare URLs, parallel topics, long debates and
image/link-led discussions. Measure Recall@k, first-page correctness, purity,
boundary loss, false merges, canonical recovery, build/query cost and bytes/unit.
Define a local encoder contract (model/recipe/dimensions, batch encode) before
selecting a model. Compare bounded SQLite BLOB search, local extensions and
rebuildable sidecars against observed scale/crash behavior. No semantic schema or
production model is frozen before these experiments justify it.

Owner direction (2026-10-01): the next encoder/store experiment uses Cloudflare
Workers AI + Vectorize rather than downloading a local model. The authorized
external inputs are the benchmark's fixed generated synthetic text and vectors
only. It reuses the operator's existing CF authentication and creates/reuses a
dedicated experiment index, with separate model/corpus/unit namespaces; it does
not modify another application's index. Real account data upload, production
semantic activation and installed runtime changes remain separate authority.
The local encoder contract and Apple baseline remain useful comparison evidence.
Cloudflare's model ID is recorded, but no immutable upstream weight revision is
exposed by this experiment; a shared dimension count never makes models compatible.

Measured synthetic result: BGE-M3 and CF Qwen3 both scored top-one 1.0 on the
unchanged eight-case baseline and separate four-case suite. 248 vectors passed
full readback; 80 real query sets matched exact local cosine and recovered fixture
identities. Temporal proxy units still mix parallel topics and lose long-debate
messages; explicit fixture reply families preserve them. Async publication needed
readback-only recovery after two bounded waits. These findings support the CF
experiment route; the owner subsequently selected **only BGE-M3 + Vectorize**
for the optional source lane. The Qwen comparison remains historical evidence,
not an active model branch. These measurements do not settle 100k-scale cost or
real-corpus acceptance. The installed production lane remains disabled. See [the generated evidence](benchmarks/README.md).

The conditional source lane uses message units with deterministic local link/reply
context expansion. Recipe v2 encodes distinct nonempty canonical fields and
excludes unknown display placeholders; skipped rows still advance bounded capture.
A previous recipe sidecar is preserved/refused until an operator replaces it while
stopped. In the unchanged global corpus, strict top-one rose from 3/13 to 5/13,
all 13 first-page hits remained, and aggregate original links moved from context4
to context3. This is an input-evidence correction, not a solved context-relevance
claim; related-topic ambiguity and context representations remain open.
The direct-seeded offline 100k manifest/canonical query benchmark rejected changed
input, recalled state and stale epoch, preserved reader state, and exposed/fixed
full-history neighbor/snapshot scans using existing indexes. Cloud ANN quality and
publication cost remain separate from those local measurements.

The optional lane stores durable encoded vectors, send intents, versioned
manifest and bounded capture checkpoints in a separate private SQLite sidecar;
remote namespace binds source account, projection epoch, model/recipe and
generation. Full float32/metadata/namespace readback plus current canonical
admission is required before publication; ambiguous uploads resume read-only.
Hard scope/time/sender/kind/watermark are remote pre-filters and local fences;
network work never holds the canonical writer transaction. An independent worker
and read capacity preserve deterministic reads. Exact account/conversation egress
consent and a Keychain token are operator configuration, not MCP inputs.

A supported lane remains optional and disabled by default, degraded on failure, fenced
by input/model/recipe generation and subject to ordinary policy/current canonical
projection. Deterministic search/read remain usable with it disabled. Publish only
when residual-case recall improves with acceptable storage/build cost.

## SG-057: Operability and integration

Content-free status reports link/lexical/semantic readiness, retrieval degradation,
indexed counts, coverage, pending batches, generation/recipe, bytes, success/failure
classes and lane counts/latency. Operator-only retrieval status/explain/rebuild
keeps ranking detail local. Storage explain distinguishes canonical, observations,
CAS, links, lexical, semantic, deliveries and backups. Pressure pauses rebuildable
work while preserving ordinary reads, replay/ACK and recovery.

Close terminal-delivery payload GC/orphans, attempt-exhausted jobs, stale indexes,
incomplete backups, config parent fsync and consistent structured MCP errors.
Status in this candidate: terminal-delivery spool GC now exists through
`operator.cache.cleanup` (bounded 500 per call, pending-publication writer fence,
24-hour aged-orphan grace), resource jobs terminalize at three attempts, incomplete
backups are preserved and never verify, the config parent directory is fsynced after
rename, and structured domain errors surface as `CallToolResult.isError=true`.
Portable Linux/full macOS synthetic workflows are implemented; local full-suite
(785 tests), final nine offline benchmark checks, build and isolated wheel installation passed. Hosted Linux (209 tests) and macOS (788 tests, 4 skips) CI passed on source
commit `ca78d4b`; publication-history/license and activation remain open. A separate
history-free 204-file source export/install and 36 focused checks passed. Authorized private
acceptance passed canonical image pixel comparison and aggregate-link/context
recovery; its preparation cost and tool latency remain separate receipts outside
Git. Audit PR, main integration, candidate tag, clean publication export and
rights decisions retain their own authority and evidence gates.

## SG-058: Selective residency and bounded on-demand reading

ReaderPolicy owns access. Independent `keep`, `recent` and `on_demand` residency
settings apply only inside authorized scope; access exclusion does not imply deletion.
New configurations and newly discovered authorized conversations default to
`on_demand`. Existing imported stock remains protected until an operator explicitly
previews and applies its release scope. Mode changes govern future collection;
old-copy release is a separate action.

`keep` is prospective; historical backfill is explicit and scoped. `recent` defaults
to 30 days and has configurable time and byte ceilings. `on_demand` has configurable
TTL and per-conversation/global byte ceilings and admits only delivered evidence
and necessary context, rather than every scanned row. Local batch settings and
content-free occupancy/release previews must expose estimates and active pins.
Cold scans remain bounded, cancellable and honest about preparation and coverage.

`wechat_find_links` and `wechat_retrieve` reuse bounded asynchronous preparation
for never-admitted, released and expired scopes. Bounded physical candidate traversal does not depend on a source time index;
canonical identity validation precedes admission of matches and bounded cross-page
neighbors/reply parents. It preserves hard scope and
never refills unrelated history. Polling uses `reading_token`; a partial result
provides a distinct idempotent continuation token to request the next bounded
attempt. A fresh request is a fresh query; materialized result cursors remain
separate. Checkpoints follow validated admission and selected-shard generations
are compared before reusing a source key. Internal source positions/bindings stay
private. Reaching the traversal boundary is not proof of one atomic complete
source snapshot. Exact budgets, token semantics and bounded-context limits are
specified in the [MCP lifecycle](MCP-CONTRACT.md#cold-linkretrieval-preparation).

All foreground/background admission paths consume the same residency decision.
Historical traversal progress and current resident coverage are separate; deliberate
eviction does not trigger automatic historical refilling. Expiring a cache cannot
silently remove actor identities, manual aliases/corrections or reader progress.
Reading an old message again must not create a new update; insufficient retained
update evidence requires explicit rebaseline/expiry. Cursors and opaque bindings
fail stale/expired when their residency view disappears. Leases and active resource/
voice jobs pin exact dependencies, not whole conversations. Pending deliveries
continue exact replay and ACK/policy semantics. Shared CAS uses all FK liveness;
no semantic remote deletion or new egress is authorized by this lifecycle.

Foreground cache reclaim and all temporary work remain in the owned-storage budget
and obey the physical free floor. Logical reusable pages and filesystem-reclaimed
bytes are different observations. A source can later lose previously evicted
content; unavailable, unscanned and partial coverage never prove absence.

Acceptance includes idle on-demand conversations without continuous body growth,
scan/admission separation, recent expiry without automatic refill, batch/access
scope boundaries, explicit backfill, update rebaseline, leases/cursor expiry,
correction identity, exact pending replay and active resource/voice dependency pins.

## SG-059: Offline compact candidate and storage evidence

Reuse the existing observation codec, storage accounting/diagnostics/history and
backup/restore namespace owner. A stopped, frozen input yields a resumable compact
candidate and an exact verified recovery point at the same committed boundary.
Legacy TEXT is encoded from its original UTF-8 bytes; valid BLOBs and all retained
logical observations keep identity, digest and sequence. Explicitly preserve
`(rowid, stable ID)` for rowid consumers and the historical `sqlite_sequence`
high-water mark; a rowid multiset alone is insufficient.

Replace stored-text FTS with contentless-delete trigram storage while preserving
casefold/literal AND truth, current-source admission, short-query fallback and hard
scope. Probe the actual runtime for SQLite ≥3.43/FTS5 support. Keep `detail=none`;
`columnsize=0` is incompatible with this contentless-delete backend. Publish a new
recipe/generation only after final message-rowid mapping and parity checks.
Normal startup must not run the large schema/backend conversion.

Runtime and DB activation are a paired stopped-only operation. External candidates
must be copied into active-filesystem staging, verified and fsynced before a same-
filesystem namespace swap. Recovery covers incomplete copy, pre/post swap and
wrong runtime/schema pairing. Do not retire the old active DB to force headroom.
Pair staging also clones pending spool, referenced CAS, the cursor signing secret
and known sidecars into an independent private namespace. An existing default-path
voice helper is independently copied with its private owner-executable mode;
source and staged identity/mode/digest are fenced before first activation. A
missing default helper stays missing, and a later appearance requires preparation
again; an explicit external helper path remains unchanged. Helper bytes share the
workspace and physical-floor accounting. Only internal storage
paths relocate; IDs/digests/payload bytes remain exact. New ACK/cache cleanup must
not erase the old pair's rollback dependencies. One atomic selection binds the
runtime, config, database and private state; the original source binding stays fixed.
Report NO-GO when old/stage/recovery/temporary bytes and the physical free floor
cannot coexist. Real candidate construction, cleanup and activation remain separate
operator authority; synthetic source completion never grants it.

The full WindowDB proof covers codec consumers, row/identity/sequence integrity,
FKs, corrections, exact replay and durable interruption recovery. The 100k lexical
reference gate adds delete/reinsert/correction churn, bounded merge/checkpoint and
observed FTS/WAL/temp footprint. Content-free preview is bounded/resumable and
separates codec distribution, logical estimates, shared page attribution, actual
candidate/operating peaks and physical headroom. Missing 7/30-day growth baselines
remain unknown; synthetic average bytes cannot become a real-account runway claim.

## Dependency sequence and remaining decisions

Evidence → resource version/provenance and search planning → versioned links →
link discovery/context retrieval → residual semantic experiments → conditional
semantic lane → operational/recovery/CI and separately authorized integration.

Implementation chooses bounded hostname forms, context windows and ranking recipe
from fixtures and query plans. Casefolded FTS5 trigram was selected as the lexical
necessary-condition accelerator after the strict-ID-set benchmark; unsupported
queries retain the bounded timeline path. Semantic units/encoder/store,
100k-scale cost/quality and any further private acceptance remain
evidence-dependent. The traced gray-block cause and decoder repair are verified.
None of these remaining items silently removes an accepted outcome.
