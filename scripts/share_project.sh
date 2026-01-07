#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="$ROOT_DIR/.share"
TS="$(date +%Y%m%d_%H%M%S)"
ARCHIVE_BASE="bodega_${TS}"
ZIP_PATH="$OUT_DIR/${ARCHIVE_BASE}.zip"
TAR_PATH="$OUT_DIR/${ARCHIVE_BASE}.tar.gz"
MANIFEST_PATH="$OUT_DIR/${ARCHIVE_BASE}_manifest.txt"

EXCLUDES=(
  ".git"
  "node_modules"
  "dist"
  "build"
  ".next"
  ".cache"
  ".DS_Store"
  "*.log"
  "coverage"
  "tmp"
  "temp"
  ".share"
)

mkdir -p "$OUT_DIR"

# Build manifest with file list (relative paths)
(
  cd "$ROOT_DIR"
  printf "Project: %s\n" "$ROOT_DIR"
  printf "Created: %s\n\n" "$(date)"
  printf "Files:\n"
  find . -type f \
    $(printf "! -path './%s/*' " "${EXCLUDES[@]}") \
    $(printf "! -name '%s' " "${EXCLUDES[@]}") \
    | sed 's|^\./||' \
    | sort
) > "$MANIFEST_PATH"

# Create archive
if command -v zip >/dev/null 2>&1; then
  (cd "$ROOT_DIR" && zip -r "$ZIP_PATH" . \
    $(printf "-x '%s' " "${EXCLUDES[@]/%/*}") \
    $(printf "-x '%s' " "${EXCLUDES[@]}")
  )
  ARCHIVE_PATH="$ZIP_PATH"
else
  (cd "$ROOT_DIR" && tar -czf "$TAR_PATH" \
    $(printf "--exclude='%s' " "${EXCLUDES[@]}") \
    .
  )
  ARCHIVE_PATH="$TAR_PATH"
fi

printf "Archive created: %s\n" "$ARCHIVE_PATH"
printf "Manifest created: %s\n" "$MANIFEST_PATH"
