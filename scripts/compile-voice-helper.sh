#!/usr/bin/env bash
# Compile the Sightglass local voice transcription helper.
#
# The helper is a Swift executable that links Apple's on-device SpeechAnalyzer /
# SpeechTranscriber APIs. It is compiled once, out of the request path, into a private
# location; the daemon only checks that the file exists and is executable.
#
# Usage: scripts/compile-voice-helper.sh [output-path]
#   output-path defaults to $SIGHTGLASS_VOICE_HELPER, or
#   "$HOME/Library/Application Support/Sightglass/voice/sightglass-transcribe".
#
# Exit codes: 0 compiled, 2 toolchain missing, 3 compile failed.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_file="$repo_root/swift/sightglass-transcribe/SightglassTranscribe.swift"
default_output="${SIGHTGLASS_VOICE_HELPER:-$HOME/Library/Application Support/Sightglass/voice/sightglass-transcribe}"
output_path="${1:-$default_output}"

if ! command -v swiftc >/dev/null 2>&1; then
  printf 'sightglass voice helper: swiftc not found (install Xcode command line tools)\n' >&2
  exit 2
fi

if [ ! -f "$source_file" ]; then
  printf 'sightglass voice helper: missing source %s\n' "$source_file" >&2
  exit 3
fi

mkdir -p "$(dirname "$output_path")"
chmod 700 "$(dirname "$output_path")"
temporary_path="$output_path.compiling.$$"
trap 'rm -f "$temporary_path"' EXIT

if ! swiftc -O -parse-as-library -target arm64-apple-macosx26.0 -o "$temporary_path" "$source_file"; then
  printf 'sightglass voice helper: compilation failed\n' >&2
  exit 3
fi

chmod 700 "$temporary_path"
mv -f "$temporary_path" "$output_path"
printf 'sightglass voice helper: wrote %s\n' "$output_path"
