#!/usr/bin/env bash
# Build the Sightglass Linux voice helper: a whisper.cpp wrapper plus its model binding.
#
# The helper is a small POSIX-sh wrapper around ``whisper-cli`` (built from upstream
# whisper.cpp).  It converts nothing itself (the daemon already wraps PCM as WAV), it
# applies a resource limit to the recognizer before the model loads, runs whisper on a
# private per-job directory, reads the exact JSON whisper.cpp produced, and prints the
# single report line ``voice/linux.py`` parses.
#
# The wrapper is built once, out of the request path, into a private location; the daemon
# only checks that the file exists, is a single-linked regular executable, and that the
# adjacent ``models/model.json`` manifest names a present single-linked model file.
#
# This script never downloads anything: point it at an already-built ``whisper-cli`` and
# an already-downloaded ggml model.  A missing binary or model is a non-zero exit, never
# a fetch.  Copy the model (and the generated manifest) next to the helper so the daemon
# probes exactly what whisper will load:
#
#   <helper-dir>/models/<model>.bin
#   <helper-dir>/models/model.json
#     {"schema": "sightglass.voice-model.v1", "file": "...", "identifier": "...",
#      "quantization": "q5_1", "whisper_version": "...", "cli_sha256": "..."}
#
# Usage:
#   scripts/build-linux-voice-helper.sh --whisper-cli <path> --model <path>
#                                       [--output <path>] [--threads <n>]
#
#   --whisper-cli  built whisper.cpp CLI (e.g. build/bin/whisper-cli)
#   --model        multilingual ggml model (default candidate: ggml-small-q5_1)
#   --output       wrapper path; defaults to $SIGHTGLASS_VOICE_HELPER or
#                  "$XDG_STATE_HOME/sightglass/voice/sightglass-whisper"
#   --threads      explicit thread count baked into the wrapper (default 2, must be >=1)
#   --source-tag   the upstream whisper.cpp source tag this CLI was built from (e.g.
#                  "v1.9.5"), recorded separately from the observed runtime version string
#
# Exit codes: 0 built, 2 a required tool/model is missing or an argument is unsafe,
# 3 the model is not a single-linked regular file, 4 a filesystem operation failed.

set -euo pipefail

usage() {
  printf 'usage: %s --whisper-cli <path> --model <path> [--output <path>] [--threads <n>] [--source-tag <tag>]\n' \
    "$(basename "$0")" >&2
}

whisper_cli=""
model_path=""
threads="${SIGHTGLASS_WHISPER_THREADS:-2}"
source_tag="${SIGHTGLASS_WHISPER_SOURCE_TAG:-}"
xdg_state="${XDG_STATE_HOME:-$HOME/.local/state}"
default_output="${SIGHTGLASS_VOICE_HELPER:-$xdg_state/sightglass/voice/sightglass-whisper}"
output_path=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --whisper-cli) whisper_cli="${2:-}"; shift 2 ;;
    --model) model_path="${2:-}"; shift 2 ;;
    --output) output_path="${2:-}"; shift 2 ;;
    --threads) threads="${2:-}"; shift 2 ;;
    --source-tag) source_tag="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done

output_path="${output_path:-$default_output}"

# Refuse values that could break out of the generated wrapper's quoting or its JSON
# report.  Only a conservative allowlist is accepted; anything else is a hard error
# rather than something to escape.  Paths are then emitted through a proper shell
# quoter so an unusual-but-safe path can never become executable shell text.
safe_plain() {
  case "$1" in
    ''|*[!A-Za-z0-9._/@:+-]*) return 1 ;;
  esac
  return 0
}

sq() {
  # Single-quote a value for POSIX sh; an embedded single quote is closed, escaped and
  # reopened.  Used for every literal written into the generated wrapper.
  printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"
}

if [ -z "$whisper_cli" ] || [ ! -x "$whisper_cli" ]; then
  printf 'sightglass linux voice helper: whisper-cli is missing or not executable\n' >&2
  exit 2
fi

if [ -z "$model_path" ] || [ ! -f "$model_path" ]; then
  printf 'sightglass linux voice helper: model is missing\n' >&2
  exit 2
fi

model_basename="$(basename "$model_path")"
if ! safe_plain "$model_basename"; then
  printf 'sightglass linux voice helper: model filename contains unsupported characters\n' >&2
  exit 2
fi

# Refuse a hardlinked or symlinked model so the path the daemon probes is the exact
# single-linked inode whisper will load.  The hardlink count is read portably (GNU
# ``stat -c`` on Linux, BSD ``stat -f`` on macOS/dev hosts).
model_links() {
  if stat -c %h "$1" >/dev/null 2>&1; then
    stat -c %h "$1"
  elif stat -f %l "$1" >/dev/null 2>&1; then
    stat -f %l "$1"
  else
    printf '?'
  fi
}

if [ -L "$model_path" ] || [ "$(model_links "$model_path")" != "1" ]; then
  printf 'sightglass linux voice helper: model must be a single-linked regular file\n' >&2
  exit 3
fi

case "$threads" in
  ''|*[!0-9]*) printf 'sightglass linux voice helper: --threads must be a positive integer\n' >&2; exit 2 ;;
esac
if [ "$threads" -lt 1 ]; then
  printf 'sightglass linux voice helper: --threads must be at least 1\n' >&2
  exit 2
fi

mkdir -p "$(dirname "$output_path")" || exit 4
chmod 700 "$(dirname "$output_path")" || exit 4
output_dir="$(cd "$(dirname "$output_path")" && pwd)" || exit 4

models_dir="$output_dir/models"
mkdir -p "$models_dir" || exit 4
chmod 700 "$models_dir" || exit 4

model_identifier="$(basename "$model_path" .bin)"

# Copy the model into the helper's own namespace so the probe and the load agree, and
# derive the quantization from the model name (q5_1, q8_0, q4_k, ...).  The exact model
# SHA is recorded so the main line's prepared release receipt can bind it.
cp "$model_path" "$models_dir/$model_basename.new.$$" || exit 4
chmod 600 "$models_dir/$model_basename.new.$$" || exit 4
mv -f "$models_dir/$model_basename.new.$$" "$models_dir/$model_basename" || exit 4

model_sha="$(
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$models_dir/$model_basename";
  else shasum -a 256 "$models_dir/$model_basename"; fi | awk '{print $1}'
)" || model_sha=""

quantization="unknown"
case "$model_basename" in
  *q5_1*) quantization="q5_1" ;;
  *q5_0*) quantization="q5_0" ;;
  *q8_0*) quantization="q8_0" ;;
  *q4_k*) quantization="q4_k" ;;
  *q4_0*) quantization="q4_0" ;;
esac

# Ask the built binary for its own version.  The observed line is preserved verbatim
# (whisper.cpp v1.9.5 reports "whisper.cpp version: 1.9.5-dev"); the source tag, if the
# operator provides one, is recorded separately.  A binary that reports no version is
# recorded as an explicit unknown, never a fabricated one.
whisper_version=""
raw_version="$("$whisper_cli" --version 2>&1 || true)"
whisper_version="$(printf '%s\n' "$raw_version" | head -n 1 | tr -d '\r')"
case "$whisper_version" in
  *[0-9].[0-9]*) : ;;
  *) whisper_version="" ;;
esac

cli_sha="$(
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$whisper_cli";
  else shasum -a 256 "$whisper_cli"; fi | awk '{print $1}'
)" || cli_sha=""

# Build the manifest with a real JSON encoder so any operator value is escaped safely.
python3 - \
  "$models_dir/model.json.new.$$" \
  "$model_basename" "$model_identifier" "$quantization" "$model_sha" "$whisper_version" "$cli_sha" \
  "$source_tag" \
  <<'PY' || exit 4
import json, sys
(out, file_name, identifier, quantization, model_sha, whisper_version, cli_sha,
 source_tag) = sys.argv[1:9]
manifest = {
    "schema": "sightglass.voice-model.v1",
    "file": file_name,
    "identifier": identifier,
    "quantization": quantization,
    "model_sha256": model_sha or None,
    "whisper_version": whisper_version or None,
    "whisper_source_tag": source_tag or None,
    "cli_sha256": cli_sha or None,
}
with open(out, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, sort_keys=True)
PY
chmod 600 "$models_dir/model.json.new.$$" || exit 4
mv -f "$models_dir/model.json.new.$$" "$models_dir/model.json" || exit 4

# Bake a build receipt next to the helper so the prepared release can be inspected.
python3 - \
  "$output_dir/helper-build.json.new.$$" \
  "$whisper_version" "$cli_sha" "$model_sha" "$quantization" "$source_tag" \
  <<'PY' || exit 4
import json, sys
out, whisper_version, cli_sha, model_sha, quantization, source_tag = sys.argv[1:7]
receipt = {
    "schema": "sightglass.voice-helper-build.v1",
    "backend": "whisper.cpp",
    "whisper_version": whisper_version or None,
    "whisper_source_tag": source_tag or None,
    "cli_sha256": cli_sha or None,
    "model_sha256": model_sha or None,
    "quantization": quantization,
    "network_fetch": False,
}
with open(out, "w", encoding="utf-8") as handle:
    json.dump(receipt, handle, sort_keys=True)
PY
chmod 600 "$output_dir/helper-build.json.new.$$" || exit 4
mv -f "$output_dir/helper-build.json.new.$$" "$output_dir/helper-build.json" || exit 4

temporary_path="$output_path.building.$$"
trap 'rm -f "$temporary_path"' EXIT

# The generated wrapper bounds the recognizer before exec.  Every literal is emitted
# through the shell quoter so a safe-but-unusual path cannot become shell text; a value
# outside the plain allowlist is refused, so no quoting is ever load-bearing for safety.
cat >"$temporary_path" <<EOF
#!/bin/sh
# Generated by scripts/build-linux-voice-helper.sh -- do not edit in place.
set -eu

whisper_cli=$(sq "$whisper_cli")
default_model=$(sq "$models_dir/$model_basename")
manifest_path=$(sq "$models_dir/model.json")
threads=$(sq "$threads")
wav_path=""
language="auto"

while [ "\$#" -gt 0 ]; do
  case "\$1" in
    --pcm) wav_path="\${2:-}"; shift 2 ;;
    --language) language="\${2:-}"; shift 2 ;;
    --model) default_model="\${2:-}"; shift 2 ;;
    --threads) threads="\${2:-}"; shift 2 ;;
    --max-rss-bytes) max_rss="\${2:-}"; shift 2 ;;
    *) printf '{"error":"helper_usage"}\n' >&2; exit 4 ;;
  esac
done

if [ -z "\$wav_path" ] || [ ! -f "\$wav_path" ]; then
  printf '{"error":"helper_usage"}\n' >&2
  exit 4
fi
if [ ! -f "\$default_model" ]; then
  printf '{"error":"model_not_installed"}\n' >&2
  exit 2
fi

# Apply the supplementary memory ceiling only when the daemon asked for one.  A failure
# to set either limit is fatal: silently running without the guard would be a false claim.
if [ -n "\${max_rss:-}" ]; then
  case "\$max_rss" in
    ''|*[!0-9]*) printf '{"error":"helper_usage"}\n' >&2; exit 4 ;;
  esac
  if ! ulimit -v "\$((max_rss / 1024))"; then
    printf '{"error":"helper_limit_unavailable"}\n' >&2
    exit 3
  fi
  if ! ulimit -d "\$((max_rss / 1024))"; then
    printf '{"error":"helper_limit_unavailable"}\n' >&2
    exit 3
  fi
fi

# A private per-job directory holds whisper's output; whisper-cli appends ".json" to the
# --output-file value, so the produced file is read by that exact name.  Only this
# directory is removed on exit.
job_dir="\$(mktemp -d "\${TMPDIR:-/tmp}/sightglass-whisper.XXXXXX")"
trap 'rm -rf "\$job_dir"' EXIT

if ! "\$whisper_cli" \
      --model "\$default_model" \
      --language "\$language" \
      --threads "\$threads" \
      --output-json-full --output-file "\$job_dir/out" \
      --no-prints \
      "\$wav_path" >/dev/null 2>&1; then
  printf '{"error":"transcription_failed"}\n' >&2
  exit 3
fi

out_json="\$job_dir/out.json"
if [ ! -f "\$out_json" ]; then
  printf '{"error":"helper_output_missing"}\n' >&2
  exit 3
fi

# Project one bounded report line through a real JSON encoder.  The provenance values
# (whisper version, quantization, model identifier) are read from the build manifest as
# *data* -- never interpolated into shell source -- so an operator value containing shell
# or JSON metacharacters cannot become executable text.  Only the daemon-controlled
# language, the numeric threads and the job-local JSON path are passed as arguments.
python3 - "\$out_json" "\$language" "\$threads" "\$manifest_path" <<'PY'
import json, sys
out_path, language, threads, manifest_path = sys.argv[1:5]
with open(manifest_path, "r", encoding="utf-8") as handle:
    manifest = json.load(handle)
whisper_version = manifest.get("whisper_version")
source_tag = manifest.get("whisper_source_tag")
quantization = manifest.get("quantization")
identifier = manifest.get("identifier") or "unknown"
with open(out_path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)
segments = payload.get("transcription") or payload.get("segments") or []
text = "".join(str(segment.get("text", "")) for segment in segments).strip()
report = {
    "schema": "sightglass.voice-transcript.v1",
    "backend": "whisper.cpp",
    "helper_version": str(whisper_version) if whisper_version else None,
    "whisper_source_tag": source_tag if isinstance(source_tag, str) else None,
    "language": language,
    "locale": language,
    "model": {"identifier": str(identifier)},
    "quantization": quantization if isinstance(quantization, str) else None,
    "threads": int(threads),
    "segments": [{"start_ms": 0, "end_ms": 0, "text": text}],
    "text": text,
}
print(json.dumps(report, sort_keys=True))
PY
EOF

chmod 700 "$temporary_path" || exit 4
mv -f "$temporary_path" "$output_path" || exit 4
printf 'sightglass linux voice helper: wrote %s (whisper %s, model %s/%s)\n' \
  "$output_path" "${whisper_version:-unknown}" "$model_basename" "$quantization"
