#!/usr/bin/env bash
# Build one private sandboxed source-folder capability helper. This never opens
# a picker, installs the bundle, edits permissions, starts a process or fetches.
#
# Usage: scripts/build-source-access-helper.sh OUTPUT.app
# Exit codes: 0 compiled, 2 missing toolchain, 3 build/sign/verification failed.

set -euo pipefail
umask 077

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_dir="$repo_root/swift/sightglass-source-access"
output_path="${1:?provide an owner-private output .app path}"

case "$output_path" in
  /*.app) ;;
  *) printf 'sightglass source helper: output must be an absolute .app path\n' >&2; exit 3 ;;
esac
if [[ -e "$output_path" || -L "$output_path" ]]; then
  printf 'sightglass source helper: output already exists\n' >&2
  exit 3
fi

for tool in swiftc codesign plutil; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    printf 'sightglass source helper: %s not found\n' "$tool" >&2
    exit 2
  fi
done

output_parent="$(dirname "$output_path")"
mkdir -p "$output_parent"
read -r parent_owner parent_mode <<< "$(stat -f '%u %Lp' "$output_parent")"
if [[ -L "$output_parent" || "$parent_owner" != "$(id -u)" ]] \
  || (( (8#$parent_mode & 077) != 0 )); then
  printf 'sightglass source helper: parent must be owner-private\n' >&2
  exit 3
fi
temporary_path="$output_path.compiling.$$"
module_cache="$(mktemp -d "${TMPDIR:-/tmp}/sightglass-source-modules.XXXXXX")"
trap 'rm -rf "$temporary_path" "$module_cache"' EXIT
mkdir -p "$temporary_path/Contents/MacOS" "$temporary_path/Contents/Resources"
cp "$source_dir/Info.plist" "$temporary_path/Contents/Info.plist"

if ! CLANG_MODULE_CACHE_PATH="$module_cache" SWIFT_MODULE_CACHE_PATH="$module_cache" \
  swiftc -O -parse-as-library \
    -target "$(uname -m)-apple-macosx13.0" \
    -o "$temporary_path/Contents/MacOS/SightglassSourceAccess" \
    "$source_dir/SightglassSourceAccess.swift"; then
  printf 'sightglass source helper: compilation failed\n' >&2
  exit 3
fi

chmod 700 "$temporary_path/Contents/MacOS/SightglassSourceAccess"
plutil -lint "$temporary_path/Contents/Info.plist" >/dev/null
codesign --force --sign - --entitlements "$source_dir/SightglassSourceAccess.entitlements" \
  "$temporary_path" >/dev/null 2>&1
codesign --verify --strict "$temporary_path" >/dev/null 2>&1
codesign -d --entitlements :- "$temporary_path" 2>/dev/null \
  | plutil -convert json -o - - \
  | grep -q '"com.apple.security.app-sandbox"[[:space:]]*:[[:space:]]*true'
codesign -d --entitlements :- "$temporary_path" 2>/dev/null \
  | plutil -convert json -o - - \
  | grep -q '"com.apple.security.files.bookmarks.app-scope"[[:space:]]*:[[:space:]]*true'
codesign -d --entitlements :- "$temporary_path" 2>/dev/null \
  | plutil -convert json -o - - \
  | grep -q '"com.apple.security.files.user-selected.read-only"[[:space:]]*:[[:space:]]*true'

chmod -R go-rwx "$temporary_path"
mv "$temporary_path" "$output_path"
printf 'sightglass source helper: wrote %s\n' "$output_path"
