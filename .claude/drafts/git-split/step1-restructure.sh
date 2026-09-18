#!/usr/bin/env bash
# step1-restructure.sh
#
# РЕПЕТИЦИЯ/ЧЕРНОВИК для mlinside-hw-olist/demo/02-dagster/PROMPT.md,
# раздел "Шаг 1. Реструктуризация git: demo -> submodule" (требования 1-7).
#
# Делает demo/ отдельным репозиторием dataengy/mlinside-demo (через
# git subtree split, история сохраняется) и подключает его как submodule
# в mlinside-hw-olist и MLInside-course.
#
# БЕЗОПАСНОСТЬ:
#   - DRY_RUN=1 по умолчанию: все МУТИРУЮЩИЕ команды (backup, subtree split,
#     clone, gh repo create, git rm/submodule add/commit) только печатаются
#     (через run()), а не выполняются.
#   - Проверки (assert_clean, gh auth status, ssh -T git@github.com) —
#     ВСЕГДА настоящие, даже при DRY_RUN=1: PROMPT.md требует останавливаться,
#     если репозитории грязные или gh не авторизован (п.1, п.4), и dry-run
#     не должен давать ложное чувство «всё готово», если это не так.
#   - Скрипт НИКОГДА сам не делает `git push` для mlinside-hw-olist или
#     MLInside-course (PROMPT.md п.7) — только печатает diff/лог и просит
#     подтверждения. `gh repo create --push` пушит НОВЫЙ репозиторий
#     mlinside-demo — это разрешено п.4 явно.
#
# РЕЖИМЫ:
#   ./step1-restructure.sh            — полный сценарий (a)-(h), с учётом DRY_RUN
#   ./step1-restructure.sh --verify   — только проверки: assert_clean x3,
#                                        gh auth status, ssh -T git@github.com
#   DRY_RUN=0 ./step1-restructure.sh  — реальные мутирующие действия
#
# ПУТИ переопределяются переменными окружения (для теста на временном клоне
# НЕ трогая реальные репозитории):
#   HW_OLIST COURSE DAGSTER_DEMO BACKUP_ROOT NEW_REPO SPLIT_BRANCH SPLIT_CLONE
#
# Проверено на macOS bash 3.2 (system /bin/bash) — без bashisms 4+
# (associative arrays, printf %()T, nameref и т.п.).

set -euo pipefail

# ---------------------------------------------------------------------------
# Пути и настройки (переопределяемые через env — см. заголовок)
# ---------------------------------------------------------------------------
HW_OLIST="${HW_OLIST:-$HOME/gi/@dataengy/mlinside-hw-olist}"
COURSE="${COURSE:-$HOME/gi/@dataengy/MLInside-course}"
DAGSTER_DEMO="${DAGSTER_DEMO:-$HOME/gi/@dataengy/mlinside-dagster-demo}"
BACKUP_ROOT="${BACKUP_ROOT:-$HOME/gi/_backup/$(date +%F)}"
NEW_REPO="${NEW_REPO:-dataengy/mlinside-demo}"
SPLIT_BRANCH="${SPLIT_BRANCH:-demo-split}"
SPLIT_CLONE="${SPLIT_CLONE:-${TMPDIR:-/tmp}/mlinside-demo-split}"
DRY_RUN="${DRY_RUN:-1}"

# ---------------------------------------------------------------------------
# log / die / run
# ---------------------------------------------------------------------------
log() { printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"; }
log_error() { printf '[%s] ERROR: %s\n' "$(date '+%H:%M:%S')" "$*" >&2; }
die() { log_error "$*"; exit 1; }

# run CMD ARGS...  — печатает команду (quoted, безопасно копипастить),
# выполняет её только при DRY_RUN=0. Использует "$@", НЕ eval/строку —
# так безопаснее с пробелами/спецсимволами в путях (~/gi/@dataengy/...).
run() {
  local out="+" a
  for a in "$@"; do
    out="${out} $(printf '%q' "$a")"
  done
  if [[ "${DRY_RUN}" == "1" ]]; then
    log "[DRY-RUN] ${out}"
  else
    log "${out}"
    "$@"
  fi
}

# ---------------------------------------------------------------------------
# Проверки (всегда настоящие, не гейтятся DRY_RUN)
# ---------------------------------------------------------------------------

# assert_clean REPO NAME — die, если репозиторий грязный или отсутствует.
assert_clean() {
  local repo="$1" name="$2" dirty n
  if [[ ! -d "${repo}" ]] || ! git -C "${repo}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    die "${name}: не найден git-репозиторий по пути ${repo}"
  fi
  dirty="$(git -C "${repo}" status --porcelain)"
  if [[ -n "${dirty}" ]]; then
    n="$(printf '%s\n' "${dirty}" | wc -l | tr -d ' ')"
    log_error "${name} (${repo}): ${n} незакоммиченных путей — см. 'git -C \"${repo}\" status'"
    die "${name} не чист. Закоммить или застэшь изменения перед реструктуризацией (PROMPT.md Step1 п.1)."
  fi
  log "OK  чисто: ${name} (${repo})"
}

# check_gh_auth — die, если gh не авторизован (PROMPT.md Step1 п.4).
check_gh_auth() {
  local out
  if out="$(gh auth status 2>&1)"; then
    log "OK  gh auth: авторизован"
    return 0
  fi
  log_error "gh auth status провалился:"
  printf '%s\n' "${out}" | sed 's/^/    /' >&2
  die "gh не авторизован — останавливаемся (PROMPT.md Step1 п.4). Подсказка: gh auth refresh -h github.com"
}

# check_ssh_github — проверка SSH до github.com (нужна для submodule add /
# push по git@github.com:... URL). Не die — вызывающий код решает сам,
# критично это или нет (в --verify это один из отчётных пунктов).
check_ssh_github() {
  local out rc=0
  out="$(ssh -T git@github.com -o ConnectTimeout=8 -o BatchMode=yes 2>&1)" || rc=$?
  if printf '%s' "${out}" | grep -qi 'successfully authenticated'; then
    log "OK  ssh git@github.com: успешная аутентификация (shell отклонён — это нормально)"
    return 0
  fi
  log_error "ssh -T git@github.com не подтвердил аутентификацию (rc=${rc}): ${out}"
  return 1
}

# ---------------------------------------------------------------------------
# --verify: только проверки (a), (e-auth), (ssh); не die на первой ошибке —
# собирает все результаты и падает в конце, если что-то не ок.
# ---------------------------------------------------------------------------
run_verify() {
  local failed=0
  log "=== --verify: только read-only проверки, никаких мутаций ==="

  if ! ( assert_clean "${HW_OLIST}" "mlinside-hw-olist" ); then failed=1; fi
  if ! ( assert_clean "${COURSE}" "MLInside-course" ); then failed=1; fi
  if ! ( assert_clean "${DAGSTER_DEMO}" "mlinside-dagster-demo" ); then failed=1; fi
  if ! ( check_gh_auth ); then failed=1; fi
  if ! check_ssh_github; then failed=1; fi

  if [[ "${failed}" -eq 0 ]]; then
    log "=== --verify: ВСЕ ПРОВЕРКИ ПРОШЛИ ==="
  else
    log_error "=== --verify: ЕСТЬ ПРОБЛЕМЫ, см. вывод выше ==="
    exit 1
  fi
}

# ---------------------------------------------------------------------------
# Стадии (a)-(h)
# ---------------------------------------------------------------------------

stage_a_assert_clean() {
  log "--- (a) git status в трёх репозиториях ---"
  assert_clean "${HW_OLIST}" "mlinside-hw-olist"
  assert_clean "${COURSE}" "MLInside-course"
  assert_clean "${DAGSTER_DEMO}" "mlinside-dagster-demo"
}

stage_b_backup() {
  log "--- (b) резервная копия ${HW_OLIST} -> ${BACKUP_ROOT} ---"
  run mkdir -p "$(dirname "${BACKUP_ROOT}")"
  run cp -R "${HW_OLIST}" "${BACKUP_ROOT}"
  log "Доп. лёгкий вариант (не заменяет cp -R, см. PROMPT.md Step1 п.2): git bundle --all"
  run git -C "${HW_OLIST}" bundle create "${BACKUP_ROOT}.bundle" --all
}

stage_c_split() {
  log "--- (c) git subtree split --prefix=demo -b ${SPLIT_BRANCH} (в ${HW_OLIST}) ---"
  run git -C "${HW_OLIST}" subtree split --prefix=demo -b "${SPLIT_BRANCH}"
}

stage_d_clone_split() {
  log "--- (d) клон ветки ${SPLIT_BRANCH} в ${SPLIT_CLONE}, переименование в main ---"
  run rm -rf "${SPLIT_CLONE}"
  run git clone --branch "${SPLIT_BRANCH}" --single-branch "${HW_OLIST}" "${SPLIT_CLONE}"
  run git -C "${SPLIT_CLONE}" branch -m "${SPLIT_BRANCH}" main
}

stage_e_publish() {
  log "--- (e) gh repo create ${NEW_REPO} --public --source=${SPLIT_CLONE} --push ---"
  check_gh_auth
  run gh repo create "${NEW_REPO}" --public --source="${SPLIT_CLONE}" --push
}

stage_f_hw_olist_submodule() {
  log "--- (f) в ${HW_OLIST}: demo/ -> submodule ${NEW_REPO} ---"
  local stash_dir="${HW_OLIST}/.stash/demo.pre-split/$(date +%F)"
  local untracked
  untracked="$(git -C "${HW_OLIST}" clean -ndx -- demo | sed -E 's/^Would remove //')"
  if [[ -n "${untracked}" ]]; then
    log "Untracked/ignored пути под demo/ будут перенесены в ${stash_dir}:"
    printf '%s\n' "${untracked}" | sed 's/^/    /'
    run mkdir -p "${stash_dir}"
    while IFS= read -r p; do
      [[ -z "${p}" ]] && continue
      run mkdir -p "${stash_dir}/$(dirname "${p}")"
      run mv "${HW_OLIST}/${p}" "${stash_dir}/${p}"
    done <<< "${untracked}"
  else
    log "Untracked/ignored путей под demo/ не найдено — переносить нечего"
  fi

  run git -C "${HW_OLIST}" rm -r demo
  run git -C "${HW_OLIST}" submodule add "git@github.com:${NEW_REPO}.git" demo

  local commit_msg
  commit_msg="refactor(demo)!: move demo/ to submodule ${NEW_REPO}

История demo/ перенесена через git subtree split в отдельный репозиторий
github.com/${NEW_REPO} и подключена как git submodule.

Untracked/ignored файлы, ранее лежавшие в demo/ (данные, кэши, локальные
конфиги), перенесены в ${stash_dir} — их нужно разнести по новому
репозиторию/секретам вручную (см. чек-лист в отчёте лейна git-split).

BREAKING CHANGE: demo/ теперь git submodule. После pull выполните:
  git submodule update --init --recursive demo"
  run git -C "${HW_OLIST}" commit -m "${commit_msg}"
}

stage_g_course_submodule() {
  log "--- (g) в ${COURSE}: demo -> submodule ${NEW_REPO} ---"
  run git -C "${COURSE}" submodule add "git@github.com:${NEW_REPO}.git" demo
  # ВАЖНО: добавляем ТОЛЬКО .gitmodules и demo — никогда git add -A
  # (правило из CLAUDE.md курса: несколько сессий в одном чекауте).
  run git -C "${COURSE}" add .gitmodules demo
  local commit_msg
  commit_msg="feat(demo): add mlinside-demo as submodule at repo root

Демо лекций (01-dbt, 02-dagster) подключены как submodule
git@github.com:${NEW_REPO}.git в корне курса ./demo.

NB: в этом репозитории теперь ЕСТЬ вложенный submodule
homework/mlinside-hw-olist/demo (тот же mlinside-demo, через
submodule mlinside-hw-olist). См. чек-лист лейна git-split
про .gitmodules update=none для вложенного пути."
  run git -C "${COURSE}" commit -m "${commit_msg}"
}

stage_h_report() {
  log "--- (h) итоговый отчёт (push НЕ выполняется этим скриптом) ---"
  log "### ${HW_OLIST}: git status --short"
  git -C "${HW_OLIST}" status --short || true
  log "### ${HW_OLIST}: git log -1"
  git -C "${HW_OLIST}" log -1 --stat || true
  log "### ${COURSE}: git status --short"
  git -C "${COURSE}" status --short || true
  log "### ${COURSE}: git log -1"
  git -C "${COURSE}" log -1 --stat || true
  log "============================================================"
  log "PUSH НЕ ВЫПОЛНЕН — ждём подтверждения пользователя перед:"
  log "  git -C '${HW_OLIST}' push"
  log "  git -C '${COURSE}' push"
  log "(gh repo create --push в шаге (e) — это отдельный НОВЫЙ репозиторий"
  log " ${NEW_REPO}, его push разрешён PROMPT.md Step1 п.4 явно)"
  log "============================================================"
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
usage() {
  cat <<EOF
Usage: DRY_RUN=1|0 $(basename "$0") [--verify]

  (без флагов)  полный сценарий (a)-(h) из PROMPT.md Step1
  --verify      только проверки: git status x3, gh auth status, ssh -T github.com

Переменные окружения (пути): HW_OLIST COURSE DAGSTER_DEMO BACKUP_ROOT
                              NEW_REPO SPLIT_BRANCH SPLIT_CLONE DRY_RUN
EOF
}

main() {
  case "${1:-}" in
    --verify)
      run_verify
      exit 0
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    "")
      ;;
    *)
      usage
      die "Неизвестный аргумент: $1"
      ;;
  esac

  log "=== step1-restructure.sh: старт (DRY_RUN=${DRY_RUN}) ==="
  log "HW_OLIST=${HW_OLIST}"
  log "COURSE=${COURSE}"
  log "DAGSTER_DEMO=${DAGSTER_DEMO}"
  log "BACKUP_ROOT=${BACKUP_ROOT}"
  log "NEW_REPO=${NEW_REPO}  SPLIT_BRANCH=${SPLIT_BRANCH}  SPLIT_CLONE=${SPLIT_CLONE}"

  stage_a_assert_clean
  stage_b_backup
  stage_c_split
  stage_d_clone_split
  stage_e_publish
  stage_f_hw_olist_submodule
  stage_g_course_submodule
  stage_h_report

  log "=== step1-restructure.sh: конец ==="
}

main "$@"
