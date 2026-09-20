"""Заготовленные патчи для DEMO (S6 «сломанный контракт», S16 «правка SQL витрины»).

    uv run python scripts/demo_patch.py break        # дубли order_id → unique(order_id) падает
    uv run python scripts/demo_patch.py sql-change   # безобидная правка SQL → меняется code_version
    uv run python scripts/demo_patch.py fix          # вернуть файл к overlay-версии

После любого режима: `just dbt-parse` + Reload definitions в UI (ADR-04b).
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

from olist_ml.settings import PROJECT_ROOT, settings

MART: Path = settings.DBT_PROJECT_DIR / "models" / "marts" / "mart_order_features.sql"
OVERLAY: Path = (
    PROJECT_ROOT / "scripts" / "dbt_overlay" / "models" / "marts" / "mart_order_features.sql"
)
MARK = "-- DEMO-PATCH"

# break: доминирующих категорий становится две → у заказов с ≥2 категориями строка удваивается.
# Модель собирается (зелёная), тест unique(order_id) падает (красный check) — контракт нарушен.
BREAK_FROM = "    where category_rank = 1\n"
BREAK_TO = f"    where category_rank <= 2  {MARK} break: две «главные» категории → дубли order_id\n"
SQL_CHANGE = (
    f"\n{MARK} sql-change: правка SQL без изменения контракта — меняется только code_version\n"
)


def apply(mode: str) -> None:
    if mode == "fix":
        shutil.copy(OVERLAY, MART)
        print(f"fix: {MART.relative_to(PROJECT_ROOT)} восстановлен из overlay")
        return
    text = MART.read_text()
    if MARK in text:
        raise SystemExit("патч уже применён — сначала `just demo-fix`")
    if mode == "break":
        if BREAK_FROM not in text:
            raise SystemExit(
                "не найден фрагмент для патча — витрина изменилась, обновите demo_patch.py"
            )
        text = text.replace(BREAK_FROM, BREAK_TO, 1)
    elif mode == "sql-change":
        text = text.rstrip() + "\n" + SQL_CHANGE
    else:
        raise SystemExit(f"неизвестный режим {mode!r}; допустимо: break | sql-change | fix")
    MART.write_text(text)
    print(f"{mode}: {MART.relative_to(PROJECT_ROOT)} изменён; дальше — `just dbt-parse` + Reload")


if __name__ == "__main__":
    apply(sys.argv[1] if len(sys.argv) > 1 else "")
