# Sightglass visual sources

Sightglass grows from its original plugin icon: two pearl-glass leaves forming a narrow open aperture. The original navy outline, pearl surfaces and mist-blue cut edges are the visual authority. Keep the mark intact and avoid an enclosing tile unless a host platform requires one.

- `mark.svg` retains the original version 1.0.0 master artwork, with explicit dimensions and language metadata added for standalone use. Keep at least 8% of its width clear on each side; use this detailed master at 48 px and above.
- `banner.png` is a 2172 × 724 raster cover generated with OpenAI image generation from the supplied plugin mark and earlier project banners. An ink-navy title field meets a matte mist-blue field carrying a nearly flat interpretation of the original two-leaf mark. An ivory caption plate crosses that seam toward the mark. Pearl fills and mist-blue cut edges retain only subtle tonal shading. The title and italic caption emphasis use generated serif lettering, with sans-serif supporting text. All lettering is part of the raster image, not live text or a bundled font.
- `visual-tokens.json` records the six original icon colors, functional text/rule colors, vector type stacks and the raster banner’s material/composition vocabulary.
- `architecture.mmd` owns the full component/edge topology. `architecture.svg` is an editable, hand-authored overview. It combines paired routes and lists credential lookups inline; the two pearl-shaped service panels echo the existing mark. The lower region shows the default-disabled semantic sidecar/service and the explicit Cloudflare egress boundary; CF candidates re-enter local admission. Update both when semantics change, following the evidence map in [Architecture](../ARCHITECTURE.md).
- SVGs embed no fonts, raster media or external resources. No WeChat logo or screenshot is used.

Keep the banner’s right field simple: the two-leaf mark, restrained shading and a quiet background. Avoid glass-card layers, reflective floors and caustic light. Keep the three text strings exactly: “Sightglass”, “Your conversations, in view.” and “READ-ONLY WECHAT · LOCAL MCP”. Break the tagline after the comma, group its metadata below the caption divider, and align all copy to the same left axis. Preserve the original silhouette and palette when revising the cover; the raster artwork does not replace the editable logo master.

Inspect the banner at its full size and at 900 px width for typography, aperture shape, clipping and contrast. Both READMEs use the same image and retain the product name and description as accessible text below it.

Inspect the architecture at 900 px width and at full size, including in grayscale. A local PNG preview can be made with `rsvg-convert --width 900 --output /tmp/sightglass-architecture.png architecture.svg`, or an equivalent installed SVG renderer. Check labels, arrow direction, ownership boundaries and clipping. Keep QA images outside Git; the SVG is the editable artifact.

Project-original independent artwork follows [CC BY-NC-SA 4.0](../../LICENSE-DOCUMENTATION.md), subject to the [scope map](../../LICENSING.md) and retained [Notices](../../NOTICE.md). No trademark or third-party rights are granted.

The architecture reader node now includes async search tokens and the private preparation job sidecar. Its paired Mermaid topology and hand-authored SVG preserve current-source result validation; the SVG was rendered and inspected after this bounded label update.

The residency label records the accepted source-body lifetime boundary. Its lease metadata and canonical message projection remain in `window.db`; pending delivery payloads retain their separate immutable spool. Implementation and installation evidence is reported in [Current state](../current-state.md).
