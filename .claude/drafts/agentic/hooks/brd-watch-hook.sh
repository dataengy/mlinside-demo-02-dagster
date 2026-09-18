#!/usr/bin/env bash
# brd-watch-hook.sh — сигналит об изменении BRD-файлов (ТЗ) на диске
#
# ── Provenance / metadata ────────────────────────────────────────────────────
# Created-at:   2026-09-18 · scaffolded by /add-session-hook, тело написано вручную
# Event:        UserPromptSubmit + SessionStart (оба регистрируются в .claude/settings.local.json)
# Contract:     print-only reminder; ВСЕГДА exit 0 — хук не имеет права блокировать сессию
# Settings:     settings.yml#session_hooks.brd-watch (рядом с этим файлом)
# Мутации:      только <repo>/.ai/.brd-state.json и <repo>/.ai/.brd-state/*.prev — больше ничего
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail

DIR="$(cd "$(dirname "$(python3 -c 'import os,sys;print(os.path.realpath(sys.argv[1]))' "${BASH_SOURCE[0]}")")" && pwd)"
SETTINGS="$DIR/settings.yml"

main() {
  command -v yq  >/dev/null 2>&1 || return 0
  command -v jq  >/dev/null 2>&1 || return 0
  command -v git >/dev/null 2>&1 || return 0

  local enabled
  enabled="$(yq -r '.session_hooks."brd-watch".enabled' "$SETTINGS" 2>/dev/null)"
  [ "$enabled" = "true" ] || return 0

  local repo
  repo="$(cd "$DIR" && git rev-parse --show-toplevel 2>/dev/null)"
  [ -n "$repo" ] || repo="$PWD"
  [ -d "$repo" ] || return 0

  local max_files diff_lines state_rel state_file state_dir
  max_files="$(yq -r '.session_hooks."brd-watch".max_files' "$SETTINGS" 2>/dev/null)"
  diff_lines="$(yq -r '.session_hooks."brd-watch".diff_lines' "$SETTINGS" 2>/dev/null)"
  state_rel="$(yq -r '.session_hooks."brd-watch".state_file' "$SETTINGS" 2>/dev/null)"
  case "$max_files"  in ''|null) max_files=20 ;; esac
  case "$diff_lines" in ''|null) diff_lines=15 ;; esac
  case "$state_rel"  in ''|null) state_rel=".ai/.brd-state.json" ;; esac
  state_file="$repo/$state_rel"
  state_dir="$repo/.ai/.brd-state"

  local patterns=()
  while IFS= read -r line; do
    [ -n "$line" ] && patterns+=("$line")
  done < <(yq -r '.session_hooks."brd-watch".patterns[]?' "$SETTINGS" 2>/dev/null)
  [ "${#patterns[@]}" -gt 0 ] || return 0

  local shacmd
  if command -v shasum >/dev/null 2>&1; then shacmd="shasum -a 256"; else shacmd="sha256sum"; fi

  local tmp_matches
  tmp_matches="$(mktemp "${TMPDIR:-/tmp}/brd-watch-matches.XXXXXX")" || return 0

  local pat core
  for pat in "${patterns[@]}"; do
    case "$pat" in
      */*)
        core="${pat#\*\*/}"
        if [ "$core" != "$pat" ]; then
          find "$repo" \( -path "$repo/.git" -o -path "$repo/.stash" -o -name node_modules \
               -o -name .venv -o -name venv -o -name dist -o -name build \
               -o -name .terraform -o -name __pycache__ \) -prune \
               -o -type f -ipath "*/$core" -print 2>/dev/null
        else
          [ -f "$repo/$pat" ] && printf '%s\n' "$repo/$pat"
        fi
        ;;
      *)
        find "$repo" \( -path "$repo/.git" -o -path "$repo/.stash" -o -name node_modules \
             -o -name .venv -o -name venv -o -name dist -o -name build \
             -o -name .terraform -o -name __pycache__ \) -prune \
             -o -type f -iname "$pat" -print 2>/dev/null
        ;;
    esac
  done | sort -u | head -n "$max_files" > "$tmp_matches"

  if [ ! -s "$tmp_matches" ]; then rm -f "$tmp_matches"; return 0; fi

  mkdir -p "$state_dir" 2>/dev/null || { rm -f "$tmp_matches"; return 0; }
  local bootstrap=0
  if [ ! -f "$state_file" ]; then
    bootstrap=1
    printf '{"files":{}}\n' > "$state_file" 2>/dev/null
  fi

  local f relpath slug prev_hash new_hash prev_snap diff_out added deleted
  while IFS= read -r f; do
    relpath="${f#"$repo"/}"
    slug="$(printf '%s' "$relpath" | tr '/ ' '__')"
    prev_snap="$state_dir/${slug}.prev"
    new_hash="$($shacmd "$f" 2>/dev/null | awk '{print $1}')"
    [ -n "$new_hash" ] || continue
    prev_hash="$(jq -r --arg p "$relpath" '.files[$p].sha256 // empty' "$state_file" 2>/dev/null)"

    if [ "$bootstrap" -eq 0 ] && [ "$prev_hash" != "$new_hash" ]; then
      if [ -f "$prev_snap" ]; then
        diff_out="$(diff -u "$prev_snap" "$f" 2>/dev/null)"
        added="$(printf '%s\n' "$diff_out" | grep -c '^+[^+]')"
        deleted="$(printf '%s\n' "$diff_out" | grep -c '^-[^-]')"
        printf '⚠️  BRD изменился: %s (+%s/-%s строк) — перечитай перед продолжением\n' "$relpath" "$added" "$deleted"
        printf '%s\n' "$diff_out" | head -n "$diff_lines"
      else
        printf '⚠️  BRD изменился (новый файл): %s — перечитай перед продолжением\n' "$relpath"
      fi
      printf '\n'
    fi

    jq --arg p "$relpath" --arg h "$new_hash" --arg t "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
      '.files[$p] = {"sha256": $h, "updated_at": $t}' "$state_file" > "$state_file.tmp" 2>/dev/null \
      && mv "$state_file.tmp" "$state_file"
    cp "$f" "$prev_snap" 2>/dev/null
  done < "$tmp_matches"

  rm -f "$tmp_matches"
  return 0
}

main || true
exit 0
