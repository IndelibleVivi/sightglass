# Notices and provenance

## Sightglass

Sightglass is a standalone local reading service in development preview. Software and functional materials are licensed under AGPL-3.0-only; independent explanatory documentation and visual assets use CC BY-NC-SA 4.0. The exact scope is in [LICENSING.md](LICENSING.md). Project material is maintained here; upstream-informed mechanisms and installed dependencies are identified below. Public visibility and an open-source license are separate states.

## WGO design and behavior audit

M0 reviewed the public repository `IndelibleVivi/we-groupchat-obsidian` at exact commit `7a492ecbb98c8061f5229b9619bfc008204bd286` (remote `main` observed 2026-09-13).

That repository identifies itself as a standalone derivative of `Qizhan7/mac-wechat-summary`, carries the GNU Affero General Public License v3 text, and preserves upstream attribution in its `NOTICE.md`. The WGO notice says the exact imported upstream baseline commit/date remains pending source audit.

Sightglass does not vendor, import, or execute the WGO runtime. For M3, `core/image_decoder.py` at the frozen commit was inspected to establish the V2 envelope behavior. For the native macOS source, `core/decryptor.py`, `core/key_extractor.py`, `core/wechat_db.py`, and `core/attachment_archive.py` were inspected for SQLCipher page-1 verification, verified key admission, current table selection, zstd content decoding, sender/outgoing behavior, bounded resource metadata, safe local-file resolution, and forwarded-record discovery/order. Sightglass's forwarded parser was informed specifically by `core/wechat_db.py` `_recordinfo_candidates` / `_parse_recordinfo`, but uses its own 256 KiB / 8-item fail-closed projection and privacy contract. Sightglass locally implements these selected mechanisms in its own provider/runtime/tests and uses installed SQLCipher for read-only database access. This is a provenance statement, not a clean-room or non-derivation claim.

The publication review maps these mechanisms to `src/sightglass/resources/v2.py`, `src/sightglass/source/macos_wechat/keys.py`, `provider.py`, `resources.py`, and `src/sightglass/source/parser.py`. In particular, the V2 decoder follows the audited envelope algorithm. Sightglass does not treat a different crypto library, stricter bounds or the absence of vendored files as proof of non-derivation. The AGPL-3.0-only software grant accommodates this provenance; the independent-content grant does not restrict or relicense AGPL-covered software. Preserve WGO and upstream attribution when distributing any affected work.

See `docs/WGO-REUSE-MAP.md` for the exact audited paths and adopted/rejected/deferred boundaries.

## agent-wechat native image-store evidence

The public repository `thisnick/agent-wechat` was inspected at exact commit `da066f501adb8454514051520007117776081332`, specifically `packages/agent-server-rust/src/tools/wechat_media.rs`, for behavioral evidence about the macOS WeChat 4.x image mapping databases, attach-store suffixes, and message-positioned thumbnail cache layout. No root license file or other reuse grant was detected at that revision.

Sightglass vendors and executes no `agent-wechat` code. It independently implements a narrower read-only resolver with its own SQLCipher snapshot verification, path confinement, single-link checks, variant contract, privacy boundaries, and tests. The publication review compared the referenced mapping functions with `src/sightglass/source/macos_wechat/image_index.py` and `resources.py`: the retained evidence is database/table/column naming, attach suffixes and message-positioned file layout; the Python bounded protobuf traversal, typed mapping results, snapshot checks and confined file reads are separately implemented. No upstream source file or documentation text is distributed here. This technical provenance finding does not invent a license for `agent-wechat` or make a clean-room claim. Any later incorporation of its code requires an actual reuse grant.

## silk-python (SILK → PCM decode)

The `voice` optional dependency installs `silk-python` 0.2.8 from PyPI (project home `https://github.com/synodriver/pysilk`, import name `pysilk`). The installed distribution declares the BSD license (`License: BSD`, classifier `License :: OSI Approved :: BSD License`).

Sightglass vendors no decoder source and copies no SILK code. The distribution is used only as an installed runtime dependency, and only inside the bounded child process `python -m sightglass.voice._decode_child`, which reads one private staging file and writes one private PCM file. The decoder works locally and makes no network request. Separately authorized resource reads may return original SILK bytes to the connected MCP client.

`silk-python` wraps the SILK v3 codec; the wrapped codec's own upstream terms were not audited for this tranche. Nothing in this notice is a redistribution grant. Before any publication or distribution that ships or bundles the decoder, the wrapped codec's licensing and the Tencent/Skype SILK terms must be reviewed deliberately, exactly like the V2/WGO boundary above.

## Project visual assets and examples

The mark under `docs/assets/` is Sightglass’s existing version 1.0.0 plugin artwork: two pearl-glass leaves forming an open aperture. The architecture composition extends that supplied project identity using editable vector geometry and system-font stacks. The raster banner was created with OpenAI image generation using the supplied project mark and palette; it is a graphic interpretation of the mark and contains no product screenshot or conversation data. Its lettering is part of the image, with no bundled font file. These assets contain no third-party icons or remote resources. See the [visual sources](docs/assets/README.md) for their formats and art direction.

The runnable example and regression data are generated synthetic fixtures; their account and person labels are placeholders, not exported conversations. Project-original examples follow the software grant, and original independent artwork follows the content grant, as mapped in [LICENSING.md](LICENSING.md); third-party rights remain unchanged.
