# Licensing scope

Licensing selection: **2026-10-05**. Public visibility, licensing, package distribution
and deployment are separate states. This map governs the project licensor's current
material; it does not claim ownership of third-party material or revoke earlier grants.

## Software and functional materials — AGPL-3.0-only

[GNU Affero General Public License version 3 only](LICENSE) applies to:

- `src/`, `tests/`, `scripts/`, `swift/`, `examples/` and `.github/`;
- `.gitignore`, `AGENTS.md`, `pyproject.toml`, `uv.lock` and `MANIFEST.in`;
- the functional implementation, build and interface contracts: `docs/SPEC.md`,
  `docs/RETRIEVAL-SPEC.md`, `docs/MCP-CONTRACT.md`, `docs/SOURCE-ADAPTER.md`,
  `docs/SECURITY.md`, `docs/OPERATIONS.md` and `docs/IMPLEMENTATION-PLAN.md`;
- executable code examples embedded in explanatory documents, even where the
  surrounding explanation follows the independent-content license below.

The software license permits commercial use and paid distribution. Distribution of
covered modified works must meet its source, notice and licensing conditions. For a
modified version supporting remote network interaction, section 13 requires an offer
of its Corresponding Source to the interacting users. This is not a noncommercial
software license, and this map adds no extra restriction to the AGPL-covered work.

## Independent explanations and artwork — CC BY-NC-SA 4.0

[CC BY-NC-SA 4.0](LICENSE-DOCUMENTATION.md) applies to project-original independent
explanatory content in `README.md`, `README.zh-CN.md`, `docs/ARCHITECTURE.md`,
`docs/READING-RELIABILITY.md`, `docs/current-state.md`, `docs/assets/` and
`docs/benchmarks/`, to the extent that copyright or other licensed rights exist.
Embedded executable examples use the software license above.

Project-original explanatory portions of `NOTICE.md` and `docs/WGO-REUSE-MAP.md`
follow this independent-content license. Quoted upstream notices, third-party
license texts and external materials retain their existing rights and terms.
Trademarks, affiliation, privacy and personality rights are not licensed here.

## Upstream and dependency boundaries

[NOTICE.md](NOTICE.md) and the [WGO reuse map](docs/WGO-REUSE-MAP.md) record exact
upstream revisions and inspected mechanisms. The software grant accommodates the
recorded WGO AGPL provenance; separate implementation, different dependencies or
absence of vendored files are not a clean-room or non-derivation claim. Preserve
WGO and its upstream attribution. No license is invented for `agent-wechat`.

Installed dependencies retain their own licenses. This source tree vendors no SILK
codec, model weights or third-party fonts. The wrapped SILK codec's redistribution
terms remain unaudited: a source publication does not authorize bundling that decoder.
Any dependency bundle needs its own terms review and retained notices.

The distribution metadata uses `AGPL-3.0-only AND CC-BY-NC-SA-4.0` because an archive
or wheel can contain both software and independently licensed explanatory content.
This expression does not offer a choice of licenses and does not put the software
under a noncommercial restriction; this path map resolves which terms apply.
