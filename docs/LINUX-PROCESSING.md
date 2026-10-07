# Linux processing and secret store

This document owns the portable Linux portion of the authorized VPS migration: the
production secret store, the image backend, and the local speech recognizer. It does not
change the macOS behavior — every Linux path selects a different backend behind the same
module contract, and the macOS Keychain / Apple `SpeechAnalyzer` / `sips` paths remain the
default on Darwin. It contains no real paths, host names, accounts, credentials or
transcripts.

Status: **candidate only, verified on the Linux target with generated inputs.** An
isolated noneditable wheel has run actual libvips rendering, Poppler PDF rendering,
FFmpeg video processing and SILK decode/WAV/whisper.cpp recognition. Controlled cgroup OOM
checks preserved a separately running reader daemon. Real-account quality, production
activation and ordinary-client acceptance remain separate. Content-free results are
recorded below; private receipts and synthesized media stay outside Git.

## Secret store (`runtime/secrets.py`)

`default_secret_store()` selects by platform and by test override:

| Condition | Store |
| --- | --- |
| `SIGHTGLASS_SYNTHETIC_TEST_SECRETS=1` | `SyntheticTestEnvironmentSecretStore` (read-only process input; tests only) |
| `sys.platform` starts with `linux` | `FileSecretStore` (owner-private files) |
| otherwise (macOS) | `KeychainSecretStore` (`/usr/bin/security`) |
| injected explicitly | `MemorySecretStore` (tests only) |

`FileSecretStore` never requires `/usr/bin/security`. It stores each secret as one
single-linked regular file inside one owner-private directory:

- the **directory is opened as a descriptor** (`O_RDONLY|O_DIRECTORY|O_NOFOLLOW`) and
  validated (owner is this UID, mode has no group/other bits) before any access; all
  reads/writes are anchored to that descriptor with `dir_fd`, so a secret is never read or
  written through a directory that was swapped for a symlink or loosened after the check;
- reads open the file `O_RDONLY|O_NOFOLLOW|O_NONBLOCK` (the non-blocking flag prevents a
  FIFO or device from hanging the reader before `fstat` proves a regular file), reject a
  non-regular file, a hardlinked file (`st_nlink != 1`), foreign ownership and non-private
  modes, and cap the read at `MAX_SECRET_BYTES`;
- the descriptor's device/inode/size/mtime/ctime/mode are captured before and after the
  read; a mismatch (replacement, truncation, append during the read) is treated as
  unavailable rather than trusted;
- writes create a `0600` temp file in the same directory, `fsync` it, `renameat` it into
  place against the validated directory descriptor, then `fsync` the directory. A failed
  write never reveals key material and never leaves a temp file;
- errors are non-revealing: a missing file, a symlink, a FIFO, an unreadable path and a
  bad mode all report the same `... secret is unavailable: <account>`.

Account names are whitelisted to a conservative character set before any path is built, so
a crafted account can never traverse out of the private directory. The default Linux
directory is `$SIGHTGLASS_SECRETS_DIR`, else `$XDG_STATE_HOME/sightglass/secrets`, else
`~/.local/state/sightglass/secrets`.

## Image processing (`resources/processors.py`)

`processor_status()` reports `sips`, `vipsheader`, `vipsthumbnail`, the PDF tools and the
bundled `ffmpeg` without claiming any of them work. Image inspect/preview dispatch by
platform:

- **macOS** uses `sips` exactly as before.
- **Linux** uses `libvips` via bounded `vipsheader` (metadata) and `vipsthumbnail`
  (preview) subprocesses. Both binaries are probed with `shutil.which` before use; if
  either is missing the read fails closed with `RESOURCE_UNAVAILABLE` /
  `image_backend_unavailable`, never a silent fallback that is not an installed
  dependency.
- Metadata comes from bounded `vipsheader --all` key/value output: `width`, `height`
  and optional `n-pages` (one when absent). MIME sniffing identifies the container;
  the header's pixel-storage type is not an image format. Dimensions are checked before
  invoking `vipsthumbnail`. Preview uses an explicit private `--output` path and
  `--size=2048x2048>` to shrink without upscaling; generated PNG bytes are sniffed and
  inspected again before admission.
- All existing guards are preserved: MIME sniffing happens before the backend runs, the
  source is passed as a `0600` no-follow file inside a private workspace, output is bounded
  by byte size and a wall-clock timeout, dimensions are validated against the pixel/pixel-
  count ceiling, and original versus thumbnail variants are unchanged. `libvips` owns the
  loader set, so HEIF/TIFF/BMP/JPEG/PNG/WebP/GIF support follows the installed
  `libheif`/`libtiff`/etc. loaders rather than a Sightglass guessing table.

The generated-preview recipe identity now derives its `processor_version` from the active
backend (`sips-v1` on macOS, `vips-v2` on Linux), so a preview cached on one platform is
not trusted as provenance for the other.

On Ubuntu 24.04 the target also required `libheif-plugin-libde265` for HEIC decoding.
A loader visible in metadata does not establish working codec support: verify actual
decode and preview for each required format. Install the decoder as an operator
dependency; Sightglass never installs it during a read.

## Local speech recognition (`voice/linux.py`)

The recognizer is an operator-prepared `whisper-cli` (upstream `whisper.cpp`) wrapped by
`scripts/build-linux-voice-helper.sh`, built once out of the request path. The pipeline is
the same one the macOS helper uses, extended for whisper.cpp:

1. **Capture** — unchanged (`voice/capture.py`), the resource service's own bounded
   snapshot read into a `0600` staging file. Capture stays canonical and happens before the
   CPU child.
2. **One bounded job child** — `python -m sightglass.voice._linux_child` runs under the
   isolated runner and performs decode, wrap and recognition in **one process tree**, so the
   cgroup memory ceiling covers the SILK decoder, the PCM/WAV buffers, the wrapper and the
   whisper model together:
   - **Decode** — bounded SILK → raw 16 kHz mono `s16le` PCM, reusing the same bounded
     helpers as `voice/_decode_child.py` (identical envelope handling, size caps,
     `RLIMIT_FSIZE` guard).
   - **Wrap** — the decoded PCM is wrapped into a WAV container in one bounded, streamed
     pass; only a 44-byte RIFF header is added, so the PCM bytes and the recorded input
     digest are unchanged and provenance still pins the exact source material.
   - **Recognize** — the prepared wrapper is invoked with an explicit language, an explicit
     thread count, a memory ceiling and its own inner watchdog that is strictly *inside* the
     outer job watchdog (so a wrapper that forked a descendant is stopped and reaped before
     the outer kill). Wrapper stdout/stderr are bounded through the shared bounded-child
     helper, and its process group is reaped on exit so no orphan outlives the job. Whisper
     consumes WAV, not raw PCM, which is why the wrap step exists.
3. **Verify** — the helper returns a single bounded report; provenance records the observed
   decode facts (real decoder/envelope/byte/frame counts) and the recognizer provenance.

### Isolation (not a daemon-wide limit; fail closed)

Production submits every helper through a runner seam (`HelperRunner`) that must genuinely
isolate the whole helper tree. There is **no automatic production fallback**:

- `SystemdRunner` launches a **named, transient `systemd-run --scope --collect` unit**
  with `MemoryAccounting=yes`, `MemoryMax=<bytes>`, `MemorySwapMax=0`, `OOMPolicy=kill`,
  `KillMode=control-group` and a bounded `RuntimeMaxSec`, so the whole helper tree lives in
  its own cgroup; a controlled OOM or the watchdog timeout stops the whole group rather
  than leaving a surviving child. `OOMPolicy=kill` is a real systemd scope property
  (systemd v255 `systemd.scope`); the earlier `MemoryOOMGroup=yes` is not a valid scope
  property and was rejected by the actual target.
- **Bounded cleanup of the owned unit.** `RuntimeMaxSec` is an additional ceiling, not
  cleanup evidence. When the outer watchdog or a stream cap kills `systemd-run` (or the
  spawn fails), the runner explicitly stops *only* its own generated 128-bit-nonce unit
  with bounded `systemctl stop`, then `systemctl kill --signal=SIGKILL`, then
  `reset-failed`, using the same system manager as launch. It checks the unit is inactive
  or absent and fails closed if cleanup cannot be proved. `RuntimeMaxSec` rounds up to
  a positive second; the inner watchdog receives the remaining outer deadline. No other
  unit is ever named and no cleanup wait is unbounded.
- When no systemd scope is available the job is **blocked** with
  `helper_runner_unavailable`; production never silently degrades to a runner that only
  applies RLIMITs. `DirectRunner` exists solely for explicitly injected synthetic/test
  wiring and reports `isolates = False`; a daemon is never built with it implicitly.

Both runners keep bounded stdout/stderr (`MAX_HELPER_STDOUT_BYTES` /
`MAX_HELPER_STDERR_BYTES`), a wall-clock deadline, and a process-group kill. The wrapper
*additionally* sets `RLIMIT_AS`/`RLIMIT_DATA` inside the child; that is a supplementary
guard and is never presented as cgroup isolation. A failure to set either requested limit is
fatal (`helper_limit_unavailable`), never silently ignored.

The helper re-asserts the private input boundary at its own seam (even though `VoiceCapture`
already verified the resource binding): the SILK file is opened
`O_RDONLY|O_NOFOLLOW|O_NONBLOCK` and
must be a private single-linked regular file, so a symlinked or hardlinked input is refused
if the helper is ever called directly.

### Threads, model and provenance

- The helper is always invoked with `--threads 2` (`HELPER_THREADS`), never whisper's
  nondeterministic hardware default. `--threads 0` is rejected at build time.
- The model is bound by an adjacent `models/model.json` manifest (`sightglass.voice-model.v1`)
  that the build script writes with a real JSON encoder:
  `{"schema": ..., "file": ..., "identifier": ..., "quantization": ...,
  "model_sha256": ..., "whisper_version": ..., "cli_sha256": ...}`. A missing, corrupt,
  wrong-schema, oversize, symlinked or non-private manifest resolves to an **unbound**
  model (`model_unresolved`) -- the recognizer never fabricates a default file name or
  quantization. An unsafe `file` value (`/`, `\\`, NUL, `.`, `..`) is refused. There is
  deliberately no second configuration owner for the model path: one explicit
  `voice.helper_path` names the whole Linux recognizer, and the manifest names the model
  next to it.
- The wrapper reads the manifest as **data** (a build-time-verified JSON file), never
  interpolating operator values into shell source, so a version or filename carrying shell
  or JSON metacharacters cannot become executable text or corrupt the report.
- Provenance records the backend (`whisper.cpp`), the helper version as the **observed
  runtime string reported by the built binary** (kept verbatim, e.g. `1.9.5-dev`; `null`
  when the binary reports none -- never a fabricated clean version), the operator-supplied
  **source tag** (`whisper_source_tag`, e.g. the upstream `v1.9.5` tag) as a *separate*
  field, the exact **manifest** model identifier and quantization (never whisper's payload
  `model.type`), the response isolation, and the thread count. The recipe engine is
  `sightglass.voice.linux-whisper.v1`; the container is recorded as `wav`. The build script
  also writes a `helper-build.json` receipt (whisper version, source tag, CLI SHA-256, model
  SHA-256, quantization, `network_fetch=false`) next to the helper. A whisper transcript can
  never be confused with the Apple `sightglass.voice.apple-silk.v1` record.

### Language mapping

The configured BCP-47 locale maps to the whisper language code, while the original locale
is preserved in provenance:

| Configured | Whisper language |
| --- | --- |
| `zh-CN`, `zh-TW`, `zh-Hans`, ... | `zh` |
| `en-US`, `en-GB`, ... | `en` |
| `auto` | `auto` |
| any bare code (`de`, `ja`, ...) | unchanged |

### Build / probe setup

```bash
# 1. Build whisper.cpp upstream and download a multilingual small quantized model
#    (default candidate: ggml-small-q5_1).  These are operator steps; Sightglass never
#    downloads a model, builds whisper.cpp, installs packages, or fetches at read time.
#    Example (operator-owned, outside Git):
#      git clone https://github.com/ggml-org/whisper.cpp
#      cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
#      # download ggml-small-q5_1.bin into a private location

# 2. Build the Sightglass wrapper into the active config's helper path.
#    helper_path empty -> <data_dir>/voice/sightglass-whisper
bash scripts/build-linux-voice-helper.sh \
  --whisper-cli /path/to/whisper.cpp/build/bin/whisper-cli \
  --model      /path/to/ggml-small-q5_1.bin \
  --source-tag v1.9.5 \
  --output     "$VOICE_HELPER"

# 3. Confirm readiness (content-free: decoder present, helper/model present and executable,
#    and a genuinely isolated runner available on a Linux host)
uv run sightglassctl status    # voice_read.readiness reports ready or a blocked_reason
```

The build script writes three private files next to the helper: the copied model, the
`models/model.json` binding manifest, and a `helper-build.json` receipt recording the
observed whisper runtime version, the operator-supplied source tag, the CLI SHA-256, the
model SHA-256 and the quantization -- the prepared release the coordinator inspects.
`probe_helper` is a pure metadata check (regular, single-link, non-empty, executable, plus a
present single-linked model named by a valid manifest) and puts no path in `status`.
With an empty `voice.helper_path`, paired preparation independently copies the default
Linux helper and its declared model/manifest/build receipt; first activation fences both
source and target bytes. An explicit external helper remains external. No real audio or
transcript is ever committed.

## Dependencies

The Linux voice extra expands the `silk-python` marker:

```toml
voice = [
  "silk-python>=0.2.8,<0.3; sys_platform == 'darwin' or sys_platform == 'linux'",
]
```

The frozen `uv.lock` now records the actual upstream cp312 Linux wheel
`silk_python-0.2.8-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl`
(sha256 `62e2e87be6b5848ab80b3384920b920deaeea90141c1494729f877a0912a7cbf`) alongside the
existing Darwin wheels, so a frozen install on the target selects the functional artifact
rather than falling back to the incomplete pure-Python source build. No hand-invented hashes
and no vendored SILK code are added; the metadata is the upstream published wheel.

The functional upstream Linux artifact recorded here is **CPython 3.12, x86_64**.
Use Python 3.12 for this Linux voice candidate; other Python/architecture combinations
have not been established by this wheel. Base synthetic/core support remains Python
3.11+. Portable CI selects 3.12 and includes the voice extra; the macOS gate retains its
existing runtime and native fixtures.

The target's hash-pinned runtime installation, `pip check`, backend import and real
SILK encode/decode all passed with this wheel. The source archive build produced an
incomplete decoder in the initial probe, so do not treat installation success or a
top-level import alone as decoder evidence.

## Platform selection summary

| Concern | macOS | Linux |
| --- | --- | --- |
| Secrets | Keychain (`/usr/bin/security`) | `FileSecretStore` (private files) |
| Image backend | `sips` | `libvips` (`vipsheader`/`vipsthumbnail`) |
| Preview recipe version | `sips-v1` | `vips-v2` |
| Recognizer | Apple `SpeechAnalyzer` helper | whisper.cpp helper |
| Recipe engine | `sightglass.voice.apple-silk.v1` | `sightglass.voice.linux-whisper.v1` |
| Helper isolation | process-group kill | systemd transient unit (cgroup) with bounded owned-unit cleanup, fail-closed + `RLIMIT_AS` |

## Verification status

- Unit tests: `tests/unit/test_linux_platform.py` covers the secret store (unsafe
  directory, FIFO, hardlink, symlink, oversize, empty, mutation-during-read, replacement),
  platform selection, the Linux image backend (capability probing, fail-closed, metadata
  parsing, oversize rejection, recipe-version selection), the whisper runner (production
  runner selection and fail-closed when systemd is absent, `OOMPolicy=kill`/`KillMode`/
  `RuntimeMaxSec` in the unit line, opaque collision-resistant nonce unit names, bounded
  `systemctl` cleanup of *only* the owned unit on timeout / stream-limit / spawn-failure
  while a clean run touches none, required memory bound), the helper contract (private
  no-follow/hardlink/mode/single-link SILK seam, model-unavailable/decode/retryable/malformed
  mapping, language/locale preservation, provenance), the manifest binding
  (missing/corrupt/wrong-schema/unsafe/symlinked/oversize all unbound), readiness branching,
  a real `_linux_child` end-to-end job (decode + WAV wrap + recognize through a stub wrapper,
  orphaned-grandchild reaping), and an end-to-end run of the real build script against a stub
  `whisper-cli` (manifest schema, build receipt with observed version + source tag, the
  correct `--output-file` `.json` read, no leftover per-job directory, zero-thread and
  unsafe-filename rejection, and a shell/JSON metacharacter version that stays data).
- Actual target, installed candidate: PNG/JPEG/TIFF/BMP/WebP/GIF/HEIC metadata and PNG
  previews passed, retaining input dimensions without upscaling. A generated two-page
  PDF returned page-two text and a rendered page that was visually inspected; a generated
  one-second MP4 returned metadata and a PNG frame.
- Actual speech pipeline: synthesized Chinese, English and mixed samples passed through
  real SILK encode/decode, WAV wrapping and whisper.cpp `small` multilingual `q5_1`.
  Source tag was `v1.9.5`; observed binary version was `1.9.5-dev`. Approximately
  9–10 second samples took 13, 13 and 25 seconds, with approximately 491 MB peak cgroup
  memory, a 2 GiB ceiling, zero swap, group OOM kill and two configured recognizer threads.
  English retained the fixed phrase and time; Chinese used traditional characters and
  had a homophone error; mixed speech misrecognized the Sightglass proper name while
  retaining API, 2026 and Zoom. These three samples do not establish real-account quality.
- Actual failure isolation: a controlled whole-job 2 GiB OOM and a real whisper job
  forced into 128 MiB both terminated their owned scopes. The separate installed daemon
  served 44 and 5 replica reads respectively during those jobs, remained the same live
  process and served another read afterwards; observed maximum read times were 17 and
  18 ms. This is bounded synthetic evidence, not a production latency guarantee.
- Actual timeout: a helper tree containing a sleeping descendant hit the 2.5-second
  outer deadline. Its owned scope was stopped in 2.591 seconds while the same daemon
  served 22 replica reads (15 ms observed maximum), and another read afterwards.
- [Public hosted gates](https://github.com/IndelibleVivi/sightglass/actions/runs/37691402170)
  at commit `a250086` passed: the full macOS suite completed 1,404 tests with four
  skips, and the portable suite completed 767 tests with two skips. The separate
  installed Linux wheel gate completed 836 tests with four platform/optional-test
  skips. The final public candidate's 135 installed Python modules and five
  license/provenance files match its verified wheel; `pip check`, CLI help and the
  installed synthetic example passed. Production transfer, activation and
  ordinary-client acceptance remain unverified.
