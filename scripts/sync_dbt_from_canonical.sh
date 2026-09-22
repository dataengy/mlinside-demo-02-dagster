#!/usr/bin/env bash
# Копия канонического dbt-проекта (mlinside-hw-olist/dbt) → ./dbt с наложением разрешённых отличий (ADR-04).
# Идемпотентно: повторный запуск даёт то же дерево. Полные CSV датасета из seeds/ канона не копируются —
# snapshot делает scripts/make_seeds_sample.py (ADR-03).
#
#   CANON=/path/to/mlinside-hw-olist/dbt just dbt-sync      # или: bash scripts/sync_dbt_from_canonical.sh
#
# Разрешённые отличия (проверяет tests/smoke/test_dbt_copy_in_sync.py): файлы из scripts/dbt_overlay/
# + dbt/seeds/raw/* + models/marts/mart_order_features.* (появятся на M2).
set -euo pipefail

# Временно отключено: dbt/ упрощается до одной витрины (mart_order_features) и её upstream —
# полный rsync из канона поверх урезанного дерева стёр бы эту структуру. Тело скрипта ниже не
# трогали — снять exit 1, когда синхронизация с каноном снова станет нужна.
echo "sync_dbt_from_canonical.sh отключён: dbt/ упрощается до одной витрины + upstream, канон временно не источник правды" >&2
exit 1

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/.." && pwd)"
canon="${CANON:-$root/../mlinside-hw-olist/dbt}"
dest="$root/dbt"
overlay="$here/dbt_overlay"

[ -f "$canon/dbt_project.yml" ] || { echo "CANON не найден: $canon" >&2; exit 1; }

rsync -a --delete \
  --exclude target --exclude dbt_packages --exclude logs --exclude .venv --exclude .idea \
  --exclude '.env' --exclude '.env.*' --exclude '.user.yml' --exclude '*.duckdb' --exclude '*.duckdb.wal' \
  --exclude 'seeds/olist_*_dataset.csv' \
  --exclude 'seeds/raw' --exclude 'models/marts/mart_order_features.sql' --exclude 'models/marts/_mart_order_features.yml' \
  "$canon/" "$dest/"

# Наложение разрешённых отличий (полные файлы, чтобы re-sync был идемпотентен).
(cd "$overlay" && find . -type f) | while read -r f; do
  mkdir -p "$dest/$(dirname "$f")"
  cp "$overlay/$f" "$dest/$f"
done

echo "dbt/ синхронизирован с $canon; отличия — из $overlay"
