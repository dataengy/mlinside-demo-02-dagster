"""DuckDB допускает одного писателя на файл — все шаги run выполняются последовательно в одном процессе (ADR-08)."""

import dagster as dg


@dg.definitions
def executor() -> dg.Definitions:
    return dg.Definitions(executor=dg.in_process_executor)
