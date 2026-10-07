# WGO reuse map

## Frozen baseline

- Repository: `https://github.com/IndelibleVivi/we-groupchat-obsidian`
- Ref resolved: remote `refs/heads/main`
- Exact commit: `7a492ecbb98c8061f5229b9619bfc008204bd286`
- Commit date/subject: 2026-09-06, `Merge pull request #22 from IndelibleVivi/fix/stable-wechat-signing-identity`
- Remote ref observed: 2026-09-13 with `git ls-remote public refs/heads/main`
- License evidence: root `LICENSE` contains GNU Affero General Public License, version 3; README/NOTICE call it `AGPL-3.0`.
- Notice evidence: root `NOTICE.md` identifies WGO as a standalone derivative of `Qizhan7/mac-wechat-summary`, preserves upstream attribution, and says its exact imported upstream SHA/date is still pending audit.

This audit reads the immutable commit object. It does not use WGO runtime state and did not read WGO config, decrypted cache, keys, logs, message data, attachments, or private continuity.

## Reviewed files

| Path at frozen commit | Why reviewed | M0/M1 disposition |
| --- | --- | --- |
| `LICENSE`, `NOTICE.md`, `README.md`, `README.zh-CN.md` | licensing and lineage | preserve provenance; no code copied |
| `AGENTS.md` | current source ownership and protected-data boundaries | treat as audit context only |
| `requirements.txt` | proven MCP package family | reuse bounded dependency `mcp[cli]>=1,<2` |
| `core/wechat_db.py` | source snapshots, shard queries, message/resource envelopes, IDs, parsing, coverage | selected current `SessionTable` / `Msg_<md5>` / zstd and bounded appmsg/image metadata behavior locally adapted for the native provider; `_recordinfo_candidates` / `_parse_recordinfo` also informed a separately bounded forwarded-record parser; reject identity/display shortcuts |
| `core/decryptor.py` | SQLCipher page layout, page-1 HMAC verification, full-file reconstruction | page-1 key verification locally adapted; full-file decryption rejected in favor of installed SQLCipher read-only connections |
| `core/source_inventory.py` | durable shard inventory and fail-closed completeness | independently implement immutable synthetic manifest inventory |
| `core/monitor_source.py` | per-shard traversal, global merge, generation admission | reuse behavioral lessons; do not import monitor state |
| `core/image_decoder.py` | MIME/V2 image behavior | behavior audited; locally implemented in M3 without WGO runtime import; not claimed clean-room |
| `core/attachment_archive.py` | safe local resolution/CAS/atomic publish | exact/numbered local-file resolution and file-identity safety behavior independently adapted for the native provider; WGO CAS adapter deferred to M5 |
| `core/key_extractor.py` | verified key admission and build profiles | verified import/profile behavior adapted; process scanning and app re-signing not imported or activated |
| `mcp_server.py` | FastMCP/stdio entrypoint and existing coupling | reuse FastMCP stack only; replace direct DB/AI/menu coupling |
| `tests/test_wechat_db.py` | WAL snapshot, stable logical message identity, paging, fail closed | translate relevant behaviors into independent synthetic tests |
| `tests/test_source_inventory.py` | missing/key/cache/generation states and path-safe evidence | translate missing/generation/privacy behaviors |
| `tests/test_monitor_source.py` | same-second ordering, multi-shard merge, cursor/generation safety | translate stable ordering and generation checks; no monitor cursor import |
| `tests/test_mcp_read_only.py`, `tests/test_mcp_identity.py` | read-only MCP and server identity | preserve read-only/no-stdout-log behavior with a new `sightglass` server |

## Adopt, reject, defer

### Adopt as independently implemented behavior

- complete source inventory before reads;
- snapshot/generation verification before and after a multi-shard read;
- physical generation identity separated from durable logical message identity;
- deterministic global merge across shards and same-second tie-breaking;
- stable source envelopes and resource descriptors captured before presentation cleanup;
- bounded V2 image envelope decode followed by MIME/content validation;
- read-only stdio MCP with no ordinary stdout logging;
- synthetic tests for missing shards, replacement generations, WAL-aware consistency lessons, and stable identity.
- complete page-1-verified key admission before native activation;
- current message-table discovery, multi-shard merge, status-based outgoing classification, and zstd content decoding for the exact supported macOS build.
- bounded native appmsg/image resource metadata plus path-confined exact/numbered local resolution, equivalent-candidate reconciliation, and pre/post file identity checks.
- bounded forwarded-chat record discovery and item ordering after inspecting WGO `core/wechat_db.py` `_recordinfo_candidates` / `_parse_recordinfo`; Sightglass uses its own 256 KiB / 8-item fail-closed projection, vendors no WGO code, and makes no clean-room claim.

### Reject from Sightglass core

- WGO source-root/path-derived namespace as account identity;
- display name, remark, nickname, handle, avatar, or first fuzzy match as canonical actor identity;
- group sender fallback such as a generic “you” label as identity evidence;
- direct MCP access to DB/config/key state;
- monitor selection, topic/event, Digest, Obsidian, AI provider, summary bookmark, or monitor cursor coupling;
- returning text strings that hide structured completeness or ambiguity.

### Defer to the owning milestone

- automatic key extraction/refresh remains deferred; the current native tranche uses an explicit verified-key-map import, exact build/source discovery, and direct read-only SQLCipher access after the M4 daemon gate;
- Indexed candidate recall + canonical validation and update cursors: implemented in M2; native candidate work is now bounded and resumable (see the MCP contract);
- Native local resource discovery, synthetic image decode, message binding and private resource cache are implemented; WGO CAS integration remains optional M5;
- WGO CAS and knowledge adapters: M5.

## Licensing boundary

No WGO files are vendored and no WGO runtime is imported. M3's V2 behavior and the native provider's page-1 verification/message schema/resource metadata/local-resolution and bounded forwarded-record behavior were locally implemented after inspecting `core/image_decoder.py`, `core/decryptor.py`, `core/key_extractor.py`, `core/wechat_db.py`, and `core/attachment_archive.py` at the frozen commit. The forwarded-record parser specifically takes behavior-level guidance from `_recordinfo_candidates` / `_parse_recordinfo` but uses Sightglass-owned limits, output contract, privacy projection, and tests. This repository records that provenance and makes no clean-room or non-derivation claim. The publication review explicitly treats the V2 algorithm and other adapted mechanisms as an AGPL provenance boundary; changed libraries and local rewrites are not used as a non-derivation claim. The current [AGPL-3.0-only software grant](../LICENSING.md) accommodates that boundary; the independent-content grant does not relicense or restrict AGPL-covered software. If WGO code is incorporated more directly later, the change must identify exact source paths/commit, preserve required notices, and resolve the repository-wide AGPL consequence before landing.
