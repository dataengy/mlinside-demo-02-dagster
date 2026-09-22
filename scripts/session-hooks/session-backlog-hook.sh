#!/usr/bin/env bash
# session-backlog-hook.sh — Reminds about undocumented/ad-hoc scripts in .claude/.tmp/ that lack a declared fate/promotion path, per session-artifacts-to-tmp
#
# ── Provenance / metadata ────────────────────────────────────────────────────
# Created-at:   2026-09-22 · scaffolded by /add-session-hook
# Event:        SessionStart (registered in .claude/settings.local.json)
# Contract:     print-only reminder; ALWAYS exit 0 — хук не имеет права блокировать сессию
# Settings:     settings.yml#session_hooks.session-backlog (рядом с этим файлом)
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail
DIR="$(cd "$(dirname "$(python3 -c 'import os,sys;print(os.path.realpath(sys.argv[1]))' "${BASH_SOURCE[0]}")")" && pwd)"
SETTINGS="$DIR/settings.yml"

K='.session_hooks.session-backlog'

main() {
  local enabled repo tmp_dir backlog_file preview
  enabled="$(yq -r "$K.enabled" "$SETTINGS" 2>/dev/null)" || return 0
  [ "$enabled" = "true" ] || return 0

  # repo root = два уровня вверх от scripts/session-hooks/
  repo="$(cd "$DIR/../.." && pwd)"
  tmp_dir="$(yq -r "$K.tmp_dir" "$SETTINGS" 2>/dev/null)"; tmp_dir="${tmp_dir:-.claude/.tmp}"
  backlog_file="$(yq -r "$K.backlog_file" "$SETTINGS" 2>/dev/null)"
  preview="$(yq -r "$K.preview" "$SETTINGS" 2>/dev/null)"; preview="${preview:-5}"

  local tmp_path="$repo/$tmp_dir"
  [ -d "$tmp_path" ] || return 0

  local backlog_text=""
  [ -n "$backlog_file" ] && [ -f "$repo/$backlog_file" ] && backlog_text="$(cat "$repo/$backlog_file" 2>/dev/null)"

  local exempt
  exempt="$(yq -r "$K.exempt[]" "$SETTINGS" 2>/dev/null)" || exempt=""

  local -a orphans=()
  local f base
  while IFS= read -r -d '' f; do
    base="$(basename "$f")"
    printf '%s\n' "$exempt" | grep -qxF "$base" && continue
    printf '%s' "$backlog_text" | grep -qF "$base" && continue
    orphans+=("$base")
  done < <(find "$tmp_path" -maxdepth 1 -type f -print0 2>/dev/null)

  [ "${#orphans[@]}" -gt 0 ] || return 0

  printf '⚙ [session-backlog] %s/%s: %s file(s) without a fate row in %s:\n' \
    "$repo" "$tmp_dir" "${#orphans[@]}" "${backlog_file:-<not set>}"
  printf '%s\n' "${orphans[@]}" | head -"$preview" | sed 's/^/⚙ [session-backlog]   /'
  printf '⚙ [session-backlog] declare a fate (skill: session-artifacts-to-tmp) or move/delete the file.\n'
}

main || true
exit 0
