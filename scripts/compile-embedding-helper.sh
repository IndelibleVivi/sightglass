#!/usr/bin/env bash
# Compile an offline NaturalLanguage encoder; no asset fetch or daemon activation.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_path="${1:?provide an owner-private output path}"
mkdir -p "$(dirname "$output_path")"
chmod 700 "$(dirname "$output_path")"
temporary_path="$output_path.compiling.$$"
trap 'rm -f "$temporary_path"' EXIT
swiftc -O -o "$temporary_path" "$repo_root/swift/sightglass-embed/main.swift"
chmod 700 "$temporary_path"
mv -f "$temporary_path" "$output_path"
