# ruff: noqa: E501 — реестр шагов читается как таблица: одна проверка на строку.
"""Проверки и прогон шагов всех демо: единая точка для `just check-demo-*`, `just demo-*-run*`, чеклистов.

Демо: 1 — Dagster с нуля · 2 — dbt · 3 — CI + Observability · 4 — ML.
Шаг демо = слайд `<a id="d{demo}-{step}">` в `demo/{demo}-2-run.md`. У каждого шага есть проверка входа
(`input`) и результата (`result`); вход может сохранить «снимок» в `demo/.state/`, результат сравнивает с ним.

CLI (из корня репозитория, окружение основного проекта):

    python demo/tests/steps.py check <demo> <step> input|result   # один шаг
    python demo/tests/steps.py ready <demo>                        # чеклист готовности демо
    python demo/tests/steps.py complete <demo>                     # чеклист результата демо
    python demo/tests/steps.py run <demo> [--step N] [--mode bash|just] [--no-checks]
    python demo/tests/steps.py list                                # все шаги
    python demo/tests/steps.py gen-just > demo/steps.just          # сгенерировать just-рецепты

`run` берёт команды из md-блоков шага: блок ```bash, где все команды начинаются с `just`, — это just-блок,
остальные — «сырые» команды. Строки `dg dev` / `mlflow ui` / `just dev` / `just mlflow` / `just demo-1-dev` и
строки с `# не выполнять` пропускаются (UI поднимается отдельно).
"""

from __future__ import annotations

import argparse
import filecmp
import json
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DEMO = REPO / "demo"
STATE = DEMO / ".state"
P1 = Path(os.getenv("DEMO1_PROJECT", DEMO / "olist_ml"))
P1_ML = P1 / "src" / "olist_ml" / "defs" / "ml.py"
M1 = DEMO / "materials" / "1"
MART_PARQUET = DEMO / "data" / "mart_order_features.parquet"
ML_VERSIONS = ("ml_v1.py", "ml_v2_metadata.py", "ml_v3_checks.py", "ml_v4_model_change.py")
DUCKDB = Path(os.getenv("DUCKDB_PATH", REPO / "data" / "olist.duckdb"))
MLFLOW_DB = Path(os.getenv("MLFLOW_DB_PATH", REPO / "data" / "mlflow.db"))
ROOT_HOME = Path(os.getenv("DAGSTER_HOME", REPO / ".dagster_home"))
MODEL_NAME = "olist_late_delivery"
ALIAS = "champion"
MART_SQL = "dbt/models/marts/mart_order_features.sql"
TITLES = {1: "Dagster с нуля", 2: "dbt", 3: "CI + Observability", 4: "ML"}

sys.path.insert(0, str(REPO / "src"))

Result = tuple[bool, str]


@dataclass
class Item:
    name: str
    fn: Callable[[], Result]
    soft: bool = False


@dataclass
class Step:
    demo: int
    step: int
    title: str
    input: list[Item] = field(default_factory=list)
    result: list[Item] = field(default_factory=list)


# ---------------------------------------------------------------- helpers


def port_open(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


def tools(*names: str) -> Result:
    miss = [n for n in names if not shutil.which(n)]
    return not miss, (f"нет в PATH: {', '.join(miss)}" if miss else ", ".join(names))


def exists(*paths: Path) -> Result:
    miss = [str(p.relative_to(REPO)) if p.is_relative_to(REPO) else str(p) for p in paths if not p.exists()]
    return not miss, (f"нет: {', '.join(miss)}" if miss else f"{len(paths)} шт.")


def same(a: Path, b: Path) -> Result:
    if not a.exists():
        return False, f"нет {a.name}"
    return filecmp.cmp(a, b, shallow=False), f"{a.name} = {b.name}"


def git_dirty(path: str) -> list[str]:
    out = subprocess.run(
        ["git", "status", "--porcelain", "--", path], cwd=REPO, capture_output=True, text=True
    ).stdout
    return [ln for ln in out.splitlines() if ln.strip()]


def models_clean() -> Result:
    d = git_dirty("dbt/models")
    return not d, ("чисто" if not d else "; ".join(d))


def duck(sql: str):
    import duckdb

    with duckdb.connect(str(DUCKDB), read_only=True) as con:
        return con.execute(sql).fetchall()


def table_rows(schema: str, table: str) -> int:
    n = duck(
        f"select count(*) from information_schema.tables where table_schema='{schema}' and table_name='{table}'"
    )[0][0]
    return duck(f"select count(*) from {schema}.{table}")[0][0] if n else -1


def mlflow_state() -> tuple[list[int], dict[str, int]]:
    with sqlite3.connect(MLFLOW_DB) as c:
        vs = [int(v) for (v,) in c.execute("select version from model_versions where name=?", (MODEL_NAME,))]
        al = dict(c.execute("select alias, version from registered_model_aliases where name=?", (MODEL_NAME,)))
    return sorted(vs), {k: int(v) for k, v in al.items()}


def instance(home: Path):
    from dagster import DagsterInstance

    if not home.exists():
        raise FileNotFoundError(f"нет DAGSTER_HOME {home}")
    return DagsterInstance.from_config(str(home))


def key(k: str):
    import dagster as dg

    return dg.AssetKey(k.split("/"))


def mats(home: Path, k: str) -> int:
    return len(instance(home).fetch_materializations(key(k), limit=1000).records)


def runs(home: Path, job: str, status: str | None = None, limit: int = 200) -> list:
    import dagster as dg

    rs = instance(home).get_runs(filters=dg.RunsFilter(job_name=job), limit=limit)
    return [r for r in rs if status is None or r.status.value == status]


def last_run(home: Path, job: str) -> str:
    rs = runs(home, job, limit=1)
    return rs[0].status.value if rs else "—"


def check_seen(home: Path, asset: str, check: str) -> set[bool]:
    import dagster as dg

    recs = instance(home).event_log_storage.get_asset_check_execution_history(
        dg.AssetCheckKey(key(asset), check), limit=200
    )
    out = set()
    for r in recs:
        data = getattr(getattr(r.event, "dagster_event", None), "event_specific_data", None)
        if hasattr(data, "passed"):
            out.add(data.passed)
    return out


def state_save(demo: int, step: int, data: dict) -> None:
    STATE.mkdir(exist_ok=True)
    (STATE / f"d{demo}-s{step}.json").write_text(json.dumps(data))


def state_load(demo: int, step: int) -> dict:
    p = STATE / f"d{demo}-s{step}.json"
    if not p.exists():
        raise FileNotFoundError(f"нет снимка входа — сначала `just check-demo-{demo}-step-{step}-input`")
    return json.loads(p.read_text())


def snap(demo: int, step: int, what: Callable[[], dict]) -> Item:
    """Пункт входа, который сохраняет снимок состояния для проверки результата."""

    def fn() -> Result:
        data = what()
        state_save(demo, step, data)
        return True, "снимок: " + ", ".join(f"{k}={v}" for k, v in data.items())

    return Item("снимок состояния для проверки результата", fn)


def C(name: str, fn: Callable[[], Result], soft: bool = False) -> Item:
    return Item(name, fn, soft)


def dg_list_defs(cwd: Path, venv: Path, home: Path) -> str:
    env = {**os.environ, "DAGSTER_HOME": str(home), "PATH": f"{venv / 'bin'}{os.pathsep}{os.environ['PATH']}"}
    return subprocess.run(
        ["dg", "list", "defs"], cwd=cwd, env=env, capture_output=True, text=True, timeout=300
    ).stdout


# ---------------------------------------------------------------- общие пункты

H1 = P1 / ".dagster_home"
ASSETS1 = ("mart_order_features", "training_dataset", "model", "model_evaluation")


def parquet_ok() -> Result:
    import pyarrow.parquet as pq

    from olist_ml.ml import features as F

    if not MART_PARQUET.exists():
        return False, "нет — `just demo-1-prepare`"
    cols = set(pq.read_schema(MART_PARQUET).names)
    miss = sorted({F.KEY, F.TARGET, *F.FEATURES} - cols)
    n = pq.read_metadata(MART_PARQUET).num_rows
    return n > 0 and not miss, f"{n} строк" + (f"; нет колонок {miss}" if miss else "")


def mats1() -> dict:
    return {a: mats(H1, a) for a in ASSETS1}


def ml_is(v: str) -> Result:
    return same(P1_ML, M1 / v)


def raw_ok() -> Result:
    n = duck("select count(*) from information_schema.tables where table_schema='raw'")[0][0]
    return n == 8, f"raw: {n} таблиц"


def mart_ok() -> Result:
    n = table_rows("marts", "mart_order_features")
    return n > 0, f"marts.mart_order_features: {n} строк"


def dq_green() -> Result:
    s = last_run(ROOT_HOME, "dq_job")
    return s == "SUCCESS", f"последний dq_job: {s}"


def registry_baseline() -> Result:
    vs, al = mlflow_state()
    return len(vs) == 1 and not al, f"версии {vs}, алиасы {al or '—'}"


def champion_set() -> Result:
    vs, al = mlflow_state()
    return ALIAS in al, f"версии {vs}, алиасы {al or '—'}"


def predictions_ok() -> Result:
    if table_rows("ml", "predictions") < 0:
        return False, "ml.predictions нет"
    rows, vers, dup = duck(
        "select count(*), count(distinct model_version), count(*) - count(distinct (order_id, batch_id)) "
        "from ml.predictions"
    )[0]
    return rows > 0 and dup == 0, f"{rows} строк, версий модели {vers}, дублей {dup}"


BASE2 = [
    C("Инструменты: dg, dbt, mlflow (venv основного проекта)", lambda: tools("dg", "dbt", "mlflow")),
    C("just в PATH", lambda: tools("just"), True),
    C(".env создан", lambda: exists(REPO / ".env"), True),
    C("DuckDB и MLflow-БД на месте", lambda: exists(DUCKDB, MLFLOW_DB)),
    C("raw-snapshot загружен (8 таблиц)", raw_ok),
    C("dbt manifest собран", lambda: exists(REPO / "dbt/target/manifest.json")),
    C("dbt/models без остатков demo-патчей", models_clean),
    C("Dagster UI слушает :3000", lambda: (port_open(3000), "")),
]

# ---------------------------------------------------------------- Демо 1

STEPS: dict[tuple[int, int], Step] = {}


def add(s: Step) -> None:
    STEPS[(s.demo, s.step)] = s


add(Step(1, 1, "Каркас и интерфейс",
    [C("Витрина parquet выгружена", parquet_ok),
     C("Проект demo/olist_ml отсутствует", lambda: (not P1.exists(), "есть — `just demo-1-reset`" if P1.exists() else "нет")),
     C("uv, uvx в PATH", lambda: tools("uv", "uvx")),
     C("direnv в PATH", lambda: tools("direnv"), True),
     C("Порт 3000 свободен", lambda: (not port_open(3000), "занят" if port_open(3000) else "свободен"))],
    [C("Проект создан: pyproject, .venv, .envrc", lambda: exists(P1 / "pyproject.toml", P1 / ".venv", P1 / ".envrc")),
     C("dg dev слушает :3000", lambda: (port_open(3000), ""), True)]))

add(Step(1, 2, "Четыре ассета → граф",
    [C("Проект создан", lambda: exists(P1 / "pyproject.toml", P1 / ".venv")),
     C("Материалы v1 и features.py на месте", lambda: exists(M1 / "ml_v1.py", REPO / "src/olist_ml/ml/features.py")),
     C("Витрина parquet выгружена", parquet_ok)],
    [C("Зависимости: pandas, pyarrow, scikit-learn",
       lambda: (all(d in (P1 / "pyproject.toml").read_text() for d in ("pandas", "pyarrow", "scikit-learn")), "")),
     C("features.py скопирован без изменений", lambda: same(P1 / "src/olist_ml/features.py", REPO / "src/olist_ml/ml/features.py")),
     C("Витрина в data/ проекта", lambda: exists(P1 / "data/mart_order_features.parquet")),
     C("defs/ml.py = v1", lambda: ml_is("ml_v1.py")),
     C("dg list defs видит 4 ассета", lambda: (lambda out: (all(a in out for a in ASSETS1), "4 ассета" if all(a in out for a in ASSETS1) else out[-300:]))(dg_list_defs(P1, P1 / ".venv", H1)))]))

add(Step(1, 3, "Materialize all",
    [C("defs/ml.py на месте", lambda: exists(P1_ML)), snap(1, 3, mats1)],
    [C("Все 4 ассета материализованы заново",
       lambda: (lambda a, b: (all(b[k] > a[k] for k in ASSETS1), str(b)))(state_load(1, 3), mats1()))]))


def roc_points() -> int:
    recs = instance(H1).fetch_materializations(key("model_evaluation"), limit=1000).records
    return sum(1 for r in recs if "roc_auc" in (r.asset_materialization.metadata or {}))


add(Step(1, 4, "Metadata",
    [C("Ассеты материализованы (D1-3)", lambda: (all(mats(H1, a) for a in ASSETS1), str(mats1())))],
    [C("defs/ml.py = v2", lambda: ml_is("ml_v2_metadata.py")),
     C("≥ 2 материализаций model_evaluation с roc_auc в metadata", lambda: (roc_points() >= 2, f"точек: {roc_points()}"))]))

add(Step(1, 5, "Asset checks",
    [C("Есть материализации с metadata (D1-4)", lambda: (roc_points() >= 1, f"точек: {roc_points()}"))],
    [C("defs/ml.py = v3", lambda: ml_is("ml_v3_checks.py")),
     C("no_leakage зелёный", lambda: (True in check_seen(H1, "training_dataset", "no_leakage"), str(check_seen(H1, "training_dataset", "no_leakage")))),
     C("quality_gate был и зелёным, и красным",
       lambda: ({True, False} <= check_seen(H1, "model_evaluation", "quality_gate"), str(check_seen(H1, "model_evaluation", "quality_gate"))))]))

add(Step(1, 6, "Выборочный пересчёт",
    [C("defs/ml.py = v3", lambda: ml_is("ml_v3_checks.py")), snap(1, 6, mats1)],
    [C("defs/ml.py = v4", lambda: ml_is("ml_v4_model_change.py")),
     C("model пересчитан, витрина и датасет — нет",
       lambda: (lambda a, b: (b["model"] > a["model"] and b["training_dataset"] == a["training_dataset"]
                              and b["mart_order_features"] == a["mart_order_features"], str(b)))(state_load(1, 6), mats1()))]))

COMPLETE1 = [
    C("Проект создан: pyproject, .venv, .envrc", lambda: exists(P1 / "pyproject.toml", P1 / ".venv", P1 / ".envrc")),
    C("features.py скопирован без изменений", lambda: same(P1 / "src/olist_ml/features.py", REPO / "src/olist_ml/ml/features.py")),
    C("defs/ml.py = v4", lambda: ml_is("ml_v4_model_change.py")),
    C("Все 4 ассета материализованы; model > training_dataset",
      lambda: (lambda m: (all(m.values()) and m["model"] > m["training_dataset"], str(m)))(mats1())),
    C("quality_gate был и зелёным, и красным",
      lambda: ({True, False} <= check_seen(H1, "model_evaluation", "quality_gate"), "")),
]

add(Step(1, 7, "Мостик ко второму демо",
    [C("defs/ml.py = v4", lambda: ml_is("ml_v4_model_change.py"))],
    [*COMPLETE1, C("Порт 3000 освобождён (dg dev остановлен)", lambda: (not port_open(3000), ""), True)]))

READY1 = [
    C("Витрина parquet выгружена, колонки feature contract на месте", parquet_ok),
    C("Проект demo/olist_ml отсутствует", lambda: (not P1.exists(), "есть — `just demo-1-reset`" if P1.exists() else "нет")),
    C("Материалы шагов на месте", lambda: exists(*(M1 / f for f in (*ML_VERSIONS, "envrc", "export_mart.py", "gate_fail.json")))),
    C("uv, uvx, direnv в PATH", lambda: tools("uv", "uvx", "direnv")),
    C("just в PATH", lambda: tools("just"), True),
    C("Порт 3000 свободен", lambda: (not port_open(3000), "занят" if port_open(3000) else "свободен")),
]

# ---------------------------------------------------------------- Демо 2 — dbt

add(Step(2, 1, "Откуда данные",
    [C("raw-snapshot загружен (8 таблиц)", raw_ok)],
    [C("raw-ассеты материализованы в Dagster", lambda: (mats(ROOT_HOME, "raw/orders") > 0, f"raw/orders: {mats(ROOT_HOME, 'raw/orders')}"), True)]))

add(Step(2, 2, "Из пустого проекта в готовый",
    [C("dbt manifest собран", lambda: exists(REPO / "dbt/target/manifest.json"))],
    [C("dg list defs видит витрину и ML", lambda: (lambda o: ("mart_order_features" in o and "model_registered" in o, "ok" if "model_registered" in o else o[-300:]))(dg_list_defs(REPO, REPO / ".venv", ROOT_HOME)))]))

add(Step(2, 3, "dbt одним компонентом",
    [C("defs.yaml компонента на месте", lambda: exists(REPO / "src/olist_ml/defs/dbt/defs.yaml"))],
    [C("manifest содержит mart_order_features",
       lambda: ("mart_order_features" in (REPO / "dbt/target/manifest.json").read_text(), ""))]))

add(Step(2, 4, "Модель dbt = ассет",
    [C("raw-snapshot загружен", raw_ok),
     snap(2, 4, lambda: {"mart": mats(ROOT_HOME, "mart_order_features")})],
    [C("Витрина в DuckDB", mart_ok),
     C("Витрина материализована заново", lambda: (mats(ROOT_HOME, "mart_order_features") > state_load(2, 4)["mart"], "")),
     C("последний feature_mart_job SUCCESS", lambda: (last_run(ROOT_HOME, "feature_mart_job") == "SUCCESS", last_run(ROOT_HOME, "feature_mart_job")))]))

add(Step(2, 5, "Тесты dbt = asset checks",
    [C("Витрина в DuckDB", mart_ok)],
    [C("последний dq_job SUCCESS (checks зелёные)", dq_green)]))

add(Step(2, 6, "Зелёная модель, красный check",
    [C("dbt/models чистые", models_clean), C("checks зелёные", dq_green),
     snap(2, 6, lambda: {"dq_fail": len(runs(ROOT_HOME, "dq_job", "FAILURE")),
                         "train_fail": len(runs(ROOT_HOME, "train_job", "FAILURE"))})],
    [C("был красный dq_job", lambda: (len(runs(ROOT_HOME, "dq_job", "FAILURE")) > state_load(2, 6)["dq_fail"], "")),
     C("train_job остановлен checks витрины", lambda: (len(runs(ROOT_HOME, "train_job", "FAILURE")) > state_load(2, 6)["train_fail"], "")),
     C("dbt/models возвращены к эталону", models_clean),
     C("checks снова зелёные", dq_green)]))


def mart_age_min() -> Result:
    recs = instance(ROOT_HOME).fetch_materializations(key("mart_order_features"), limit=1).records
    if not recs:
        return False, "витрина не материализована"
    return True, f"с последней материализации {int((time.time() - recs[0].timestamp) / 60)} мин"


add(Step(2, 7, "Freshness",
    [C("Витрина материализована", lambda: (mats(ROOT_HOME, "mart_order_features") > 0, ""))],
    [C("Возраст витрины (окна WARN/FAIL — FRESHNESS_*_MIN в .env)", mart_age_min, True)]))

add(Step(2, 8, "Резерв dbt-блока",
    [C("dbt/models чистые", models_clean)],
    [C("checks зелёные", dq_green)]))

READY2 = [*BASE2]
COMPLETE2 = [
    C("Витрина в DuckDB", mart_ok),
    C("checks витрины зелёные", dq_green),
    C("был красный dq_job (S6)", lambda: (len(runs(ROOT_HOME, "dq_job", "FAILURE")) > 0, "")),
    C("dbt/models возвращены к эталону", models_clean),
]

# ---------------------------------------------------------------- Демо 3 — CI + Observability

SENSORS = REPO / "src/olist_ml/defs/automation/sensors.py"

add(Step(3, 1, "CI: что именно зелёное",
    [C("ci.yml на месте", lambda: exists(REPO / ".github/workflows/ci.yml")),
     C("uv, dbt, dg, ruff, pytest", lambda: tools("uv", "dbt", "dg", "ruff", "pytest"))],
    [C("ruff check . зелёный", lambda: (subprocess.run(["ruff", "check", "."], cwd=REPO, capture_output=True).returncode == 0, "")),
     C("manifest собран", lambda: exists(REPO / "dbt/target/manifest.json"))]))

add(Step(3, 2, "Observability: run упал",
    [C("dbt/models чистые", models_clean), C("checks зелёные", dq_green),
     C("сенсоры определены", lambda: (all(s in SENSORS.read_text() for s in ("alert_on_run_failure", "alert_on_failed_check")), "")),
     snap(3, 2, lambda: {"dq_fail": len(runs(ROOT_HOME, "dq_job", "FAILURE"))})],
    [C("был красный dq_job", lambda: (len(runs(ROOT_HOME, "dq_job", "FAILURE")) > state_load(3, 2)["dq_fail"], "")),
     C("витрина сломана патчем break", lambda: (bool(git_dirty(MART_SQL)), "; ".join(git_dirty(MART_SQL)) or "чисто"))]))

add(Step(3, 3, "Observability: run зелёный, check жёлтый",
    [C("витрина под патчем break (после D3-2) или чистая", lambda: (True, "; ".join(git_dirty(MART_SQL)) or "чисто")),
     snap(3, 3, lambda: {"dq_ok": len(runs(ROOT_HOME, "dq_job", "SUCCESS"))})],
    [C("dq_job зелёные после break-warn и после fix", lambda: (len(runs(ROOT_HOME, "dq_job", "SUCCESS")) >= state_load(3, 3)["dq_ok"] + 2, "")),
     C("dbt/models возвращены к эталону", models_clean)]))

READY3 = [*BASE2, C("Витрина в DuckDB", mart_ok), C("checks витрины зелёные", dq_green),
          C("ci.yml на месте", lambda: exists(REPO / ".github/workflows/ci.yml"))]
COMPLETE3 = [C("dbt/models возвращены к эталону", models_clean), C("checks витрины зелёные", dq_green),
             C("был красный dq_job", lambda: (len(runs(ROOT_HOME, "dq_job", "FAILURE")) > 0, ""))]

# ---------------------------------------------------------------- Демо 4 — ML


def split_ok() -> Result:
    known, unknown = duck(
        "select count(*) filter (where is_late_delivery is not null), count(*) filter (where is_late_delivery is null) "
        "from marts.mart_order_features"
    )[0]
    return known > 0 and unknown > 0, f"target известен: {known}, неизвестен: {unknown}"


def no_leak() -> Result:
    from olist_ml.ml import features as F

    bad = {F.TARGET, "delivery_delay_days"} & set(F.FEATURES)
    return not bad, f"{len(F.FEATURES)} признаков" + (f"; утечка: {bad}" if bad else "")


add(Step(4, 1, "Три набора строк",
    [C("Витрина в DuckDB", mart_ok)], [C("есть строки с известным и неизвестным target", split_ok)]))
add(Step(4, 2, "Утечка",
    [C("Витрина в DuckDB", mart_ok)], [C("target и delivery_delay_days не в признаках", no_leak)]))
add(Step(4, 3, "Обучение",
    [C("checks витрины зелёные", dq_green), snap(4, 3, lambda: {"versions": len(mlflow_state()[0])})],
    [C("новая версия в реестре", lambda: (len(mlflow_state()[0]) > state_load(4, 3)["versions"], str(mlflow_state()[0]))),
     C("последний train_job SUCCESS", lambda: (last_run(ROOT_HOME, "train_job") == "SUCCESS", last_run(ROOT_HOME, "train_job"))),
     C("snapshot train/holdout в data/ml", lambda: (bool(list((REPO / "data/ml").glob("train_*.parquet"))), ""))]))
add(Step(4, 4, "Gate падает",
    [snap(4, 4, lambda: {"versions": len(mlflow_state()[0]), "alias": mlflow_state()[1].get(ALIAS)})],
    [C("новой версии нет, alias не тронут",
       lambda: (lambda s: (len(mlflow_state()[0]) == s["versions"] and mlflow_state()[1].get(ALIAS) == s["alias"], str(mlflow_state())))(state_load(4, 4))),
     C("последний train_job FAILURE (gate)", lambda: (last_run(ROOT_HOME, "train_job") == "FAILURE", last_run(ROOT_HOME, "train_job")))]))
add(Step(4, 5, "Регистрация ≠ promotion",
    [C("в реестре ≥ 2 версий", lambda: (len(mlflow_state()[0]) >= 2, str(mlflow_state()[0])))],
    [C("champion → последняя версия", lambda: (lambda vs, al: (al.get(ALIAS) == max(vs), f"{vs}, {al}"))(*mlflow_state()))]))
add(Step(4, 6, "Batch inference",
    [C("champion выставлен", champion_set)],
    [C("predictions: строки, без дублей", predictions_ok),
     C("model_version в predictions = champion",
       lambda: (mlflow_state()[1].get(ALIAS) in {v for (v,) in duck("select distinct model_version from ml.predictions")}, ""))]))
add(Step(4, 7, "Без champion",
    [C("champion выставлен", champion_set), snap(4, 7, lambda: {"score_fail": len(runs(ROOT_HOME, "score_job", "FAILURE"))})],
    [C("был понятный провал score_job", lambda: (len(runs(ROOT_HOME, "score_job", "FAILURE")) > state_load(4, 7)["score_fail"], "")),
     C("champion возвращён", champion_set)]))
add(Step(4, 8, "Меняем SQL витрины",
    [C("dbt/models чистые", models_clean)],
    [C("SQL витрины изменён", lambda: (bool(git_dirty(MART_SQL)), "; ".join(git_dirty(MART_SQL)) or "чисто"))]))


def counts4() -> dict:
    return {k: mats(ROOT_HOME, k) for k in ("mart_order_features", "model_registered", "raw/orders", "stg_orders")}


add(Step(4, 9, "Выборочный пересчёт",
    [C("SQL витрины изменён (D4-8)", lambda: (bool(git_dirty(MART_SQL)), "")), snap(4, 9, counts4)],
    [C("витрина и model_registered пересчитаны; raw и staging — нет",
       lambda: (lambda a, b: (b["mart_order_features"] > a["mart_order_features"] and b["model_registered"] > a["model_registered"]
                              and b["raw/orders"] == a["raw/orders"] and b["stg_orders"] == a["stg_orders"], str(b)))(state_load(4, 9), counts4()))]))
add(Step(4, 10, "Dev vs prod",
    [C("predictions есть", predictions_ok)],
    [C("dbt/models возвращены к эталону (вне кадра: demo-fix)", models_clean)]))

READY4 = [*BASE2, C("Витрина в DuckDB", mart_ok), C("checks витрины зелёные", dq_green),
          C("Реестр: одна baseline-версия, alias пуст", registry_baseline),
          C("MLflow UI слушает :5001", lambda: (port_open(5001), "")),
          C("predictions ещё нет", lambda: (table_rows("ml", "predictions") < 0, ""), True)]
COMPLETE4 = [C("Реестр: ≥ 2 версий, champion выставлен", lambda: (len(mlflow_state()[0]) >= 2 and ALIAS in mlflow_state()[1], str(mlflow_state()))),
             C("predictions: строки, без дублей", predictions_ok),
             C("dbt/models возвращены к эталону", models_clean, True)]

CHECKLISTS = {("ready", 1): READY1, ("complete", 1): COMPLETE1, ("ready", 2): READY2, ("complete", 2): COMPLETE2,
              ("ready", 3): READY3, ("complete", 3): COMPLETE3, ("ready", 4): READY4, ("complete", 4): COMPLETE4}

# ---------------------------------------------------------------- runner


def report(title: str, items: list[Item]) -> bool:
    print(f"\n== {title}")
    failed = 0
    for it in items:
        try:
            ok, detail = it.fn()
        except Exception as e:  # пункт не роняет отчёт
            ok, detail = False, f"{type(e).__name__}: {e}"
        print(("✅" if ok else "⚠️ " if it.soft else "❌") + f" {it.name}" + (f" — {detail}" if detail else ""))
        failed += (not ok) and not it.soft
    print("-- ГОТОВО" if not failed else f"-- провалено пунктов: {failed}")
    return not failed


def run_doc(demo: int) -> Path:
    return DEMO / f"{demo}-2-run.md"


def blocks(demo: int, step: int) -> tuple[list[str], list[str]]:
    text = run_doc(demo).read_text()
    m = re.search(rf'<a id="d{demo}-{step}"></a>(.*?)(?=<a id="d{demo}-\d+"></a>|\Z)', text, re.S)
    if not m:
        raise SystemExit(f"шаг d{demo}-{step} не найден в {run_doc(demo).name}")
    raw, just = [], []
    for b in re.findall(r"^```bash\n(.*?)^```", m.group(1), re.S | re.M):
        cmds = [ln for ln in b.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]
        (just if cmds and all(c.lstrip().startswith("just ") for c in cmds) else raw).extend(b.splitlines())
    return raw, just


SKIP = re.compile(r"^\s*(dg dev\b|mlflow ui\b|just (dev|mlflow|demo-1-dev)\b)|# не выполнять")
PRELUDE = """set -eo pipefail
command -v direnv >/dev/null || direnv() { :; }
command -v just >/dev/null || just() { uvx --from rust-just just "$@"; }
dbt_parse() { dbt parse --quiet --project-dir "$PWD/dbt" --profiles-dir "$PWD/dbt" --target-path "$PWD/dbt/target"; }
"""


def exec_lines(demo: int, step: int, lines: list[str]) -> bool:
    todo = []
    for ln in lines:
        if SKIP.search(ln):
            print(f"   ⏭  пропущено (UI/фон): {ln.strip()}")
        else:
            todo.append(ln)
    if not any(ln.strip() and not ln.lstrip().startswith("#") for ln in todo):
        print("   (команд нет)")
        return True
    if demo == 1 and step > 1:
        cwd, venv, home = P1, P1 / ".venv", P1 / ".dagster_home"
    else:
        cwd, venv, home = REPO, REPO / ".venv", ROOT_HOME
    env = {**os.environ, "DAGSTER_HOME": str(home), "VIRTUAL_ENV": str(venv),
           "PATH": f"{venv / 'bin'}{os.pathsep}{os.environ['PATH']}"}
    home.mkdir(parents=True, exist_ok=True)
    print("   $ " + "\n   $ ".join(ln for ln in todo if ln.strip()))
    r = subprocess.run(["bash", "-c", PRELUDE + "\n".join(todo)], cwd=cwd, env=env)
    print(f"   → код выхода {r.returncode}")
    return r.returncode == 0


def run(demo: int, only: int | None, mode: str, checks: bool) -> bool:
    ok = True
    for (d, s), st in sorted(STEPS.items()):
        if d != demo or (only and s != only):
            continue
        print(f"\n######## D{d}-{s}. {st.title} [{mode}]")
        if checks:
            ok &= report(f"D{d}-{s} вход", st.input)
        raw, just = blocks(d, s)
        ok &= exec_lines(d, s, raw if mode == "bash" else just)
        if checks:
            ok &= report(f"D{d}-{s} результат", st.result)
    print("\n==== ИТОГ: " + ("всё зелёное" if ok else "есть провалы"))
    return ok


def gen_just() -> str:
    out = ["# СГЕНЕРИРОВАНО: python demo/tests/steps.py gen-just > demo/steps.just — не править руками.",
           "# Проверки входа/результата каждого шага демо (demo/{N}-2-run.md, «Дано» / «Результат»).", ""]
    for (d, s), st in sorted(STEPS.items()):
        for kind, word in (("input", "вход"), ("result", "результат")):
            out += [f"# Демо {d} ({TITLES[d]}), шаг D{d}-{s} «{st.title}»: проверка — {word}",
                    f"check-demo-{d}-step-{s}-{kind}:",
                    f'    cd "{{{{demo_root}}}}" && uv run python demo/tests/steps.py check {d} {s} {kind}', ""]
    return "\n".join(out)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check")
    c.add_argument("demo", type=int), c.add_argument("step", type=int), c.add_argument("kind", choices=["input", "result"])
    for n in ("ready", "complete"):
        sub.add_parser(n).add_argument("demo", type=int)
    r = sub.add_parser("run")
    r.add_argument("demo", type=int), r.add_argument("--step", type=int), r.add_argument("--mode", default="bash", choices=["bash", "just"])
    r.add_argument("--no-checks", action="store_true")
    sub.add_parser("list"), sub.add_parser("gen-just")
    a = ap.parse_args()
    if a.cmd == "check":
        st = STEPS[(a.demo, a.step)]
        ok = report(f"D{a.demo}-{a.step} «{st.title}»: {'вход' if a.kind == 'input' else 'результат'}", getattr(st, a.kind))
    elif a.cmd in ("ready", "complete"):
        ok = report(f"Демо {a.demo} ({TITLES[a.demo]}) — {'готовность' if a.cmd == 'ready' else 'итог'}", CHECKLISTS[(a.cmd, a.demo)])
    elif a.cmd == "run":
        ok = run(a.demo, a.step, a.mode, not a.no_checks)
    elif a.cmd == "list":
        for (d, s), st in sorted(STEPS.items()):
            print(f"D{d}-{s}\t{TITLES[d]}\t{st.title}")
        ok = True
    else:
        print(gen_just())
        ok = True
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
