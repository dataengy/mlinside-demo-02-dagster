#!/usr/bin/env bash
# apply.sh — устанавливает hook brd-watch + skill-кандидат reread-brd-and-sync-plan
#            + agent brd-watcher, собранные в этом LANE, потому что песочница
#            текущей сессии запретила писать напрямую в ~/.claude/**, ~/.ai/**
#            и в .claude/** целевого репозитория (Operation not permitted).
#
# Идемпотентно: безопасно запускать повторно (add-session-hook.sh и cp — SKIP/
# перезапись одного и того же контента; symlink -f).
#
# Запуск с дефолтным репо (mlinside-hw-olist, ДО выделения mlinside-demo):
#   bash apply.sh
# После того как репозиторий mlinside-demo будет выделен — перенести хук туда:
#   REPO=/path/to/mlinside-demo bash apply.sh
#
set -euo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="${REPO:-/Users/user/gi/@dataengy/mlinside-hw-olist}"
JF="$HOME/.ai/skills/_scripts/session/hooks/Justfile"
DESC="BRD (ТЗ) изменился на диске — сигнал перечитать перед продолжением"

echo "== 0. проверка репо =="
[ -d "$REPO/.git" ] || { echo "ERROR: $REPO не похож на git-репозиторий (нет .git)"; exit 1; }
echo "REPO=$REPO"

echo
echo "== 1. hook brd-watch: регистрация на UserPromptSubmit (создаёт скелет+settings.yml) =="
just -f "$JF" add-session-hook-apply brd-watch --desc "$DESC" --event UserPromptSubmit --repo "$REPO"

echo
echo "== 2. hook brd-watch: подмена тела на готовую реализацию (протестирована локально) =="
cp "$SELF_DIR/hooks/brd-watch-hook.sh" "$REPO/scripts/session-hooks/brd-watch-hook.sh"
chmod +x "$REPO/scripts/session-hooks/brd-watch-hook.sh"

echo
echo "== 3. settings.yml: добавляем patterns/max_files/diff_lines/state_file (SSoT, не в коде) =="
yq -i '
  .session_hooks."brd-watch".patterns = [
    "PROMPT.md", "**/PROMPT.md",
    "BRD*.md",   "**/BRD*.md",
    "PRD*.md",   "**/PRD*.md",
    "SPEC*.md",  "**/SPEC*.md",
    "PLAN.md",   "**/PLAN.md",
    "docs/decisions.md", "**/docs/decisions.md"
  ] |
  .session_hooks."brd-watch".max_files = 20 |
  .session_hooks."brd-watch".diff_lines = 15 |
  .session_hooks."brd-watch".state_file = ".ai/.brd-state.json"
' "$REPO/scripts/session-hooks/settings.yml"

echo
echo "== 4. hook brd-watch: регистрация на SessionStart (тот же скрипт, второе событие) =="
just -f "$JF" add-session-hook-apply brd-watch --desc "$DESC" --event SessionStart --repo "$REPO"

echo
echo "== 5. .gitignore: стамп/снэпшоты BRD не должны коммититься =="
GITIGNORE="$REPO/.gitignore"
if ! grep -qxF '.ai/.brd-state.json' "$GITIGNORE" 2>/dev/null; then
  { echo ''; echo '# brd-watch hook state (см. .claude/skills/add-session-hook)'; \
    echo '.ai/.brd-state.json'; echo '.ai/.brd-state/'; } >> "$GITIGNORE"
  echo "добавлено в $GITIGNORE"
else
  echo "SKIP — .gitignore уже содержит .ai/.brd-state.json"
fi

echo
echo "== 6. pipe-test =="
echo '{}' | bash "$REPO/scripts/session-hooks/brd-watch-hook.sh"
echo "exit=$? (ожидается 0)"

echo
echo "== 7. skill-кандидат reread-brd-and-sync-plan (scope=global, prj=mlinside-demo) =="
CAND_DIR="$HOME/.ai/skills/_skills_candidates_glob/mlinside-demo"
HLS_DIR="$HOME/.ai/skills/skills-candidates-ai-HLs"
mkdir -p "$CAND_DIR" "$HLS_DIR"
cp "$SELF_DIR/skills/reread-brd-and-sync-plan.candidate.md" "$CAND_DIR/reread-brd-and-sync-plan.md"
ln -sf "$CAND_DIR/reread-brd-and-sync-plan.md" "$HLS_DIR/glob-reread-brd-and-sync-plan.md"
echo "candidate: $CAND_DIR/reread-brd-and-sync-plan.md"
echo "symlink  : $HLS_DIR/glob-reread-brd-and-sync-plan.md"
echo "Чтобы промоутнуть в настоящий скилл: /create-skill reread-brd-and-sync-plan"

echo
echo "== 8. agent brd-watcher — авторим в проекте, синкаем dry-run (по правилам sync-project-agents) =="
mkdir -p "$REPO/.claude/agents"
cp "$SELF_DIR/agents/brd-watcher.md" "$REPO/.claude/agents/brd-watcher.md"
echo "authored: $REPO/.claude/agents/brd-watcher.md"
AGENTS_JF="$HOME/.ai/skills/_scripts/agents/Justfile"
if [ -f "$AGENTS_JF" ]; then
  echo "--- dry-run (ничего не пишет; посмотреть план и классификацию) ---"
  just -f "$AGENTS_JF" sync-project-agents --project-dir "$REPO" || true
  echo "Когда план устроит — применить вручную:"
  echo "  just -f $AGENTS_JF sync-project-agents --project-dir \"$REPO\" --apply"
else
  echo "WARN: $AGENTS_JF не найден — синк агента сделать вручную по /sync-project-agents"
fi

echo
echo "== ГОТОВО =="
echo "Хук:   $REPO/scripts/session-hooks/brd-watch-hook.sh (UserPromptSubmit + SessionStart)"
echo "Скилл: candidate → $CAND_DIR/reread-brd-and-sync-plan.md (промоут: /create-skill)"
echo "Агент: $REPO/.claude/agents/brd-watcher.md (синк в каталог — dry-run выше, apply вручную)"
echo
echo "После выделения репозитория mlinside-demo: REPO=/path/to/mlinside-demo bash apply.sh"
