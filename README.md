# mlinside-demo-02-dagster

Демо-проект к лекции MLInside «Оркестрация ML-пайплайнов на Dagster» на датасете
[Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Четыре демо по нарастающей:

1. **Ingest** — `create-dagster` → `dg dev`; v1 `dbt seed`, v2 dlt из Kaggle с сэмплированием.
2. **dbt** — `DbtProjectComponent`, трансформация и DQ как раздельные стадии.
3. **ML** — фичи-витрина → обучение → MLflow, гейт качества.
4. **CI / Observability** — GitHub Actions, Docker Compose, Prometheus/Loki/Grafana, алерты в Telegram.

## Статус

Репозиторий на стадии плана: код появится по шагам из [`.claude/PLAN.md`](.claude/PLAN.md).
Постановка задачи — [`.claude/PROMPT.md`](.claude/PROMPT.md); результаты лейнов-разведки — `.claude/drafts/`.

## Происхождение

Выделен из [dataengy/mlinside-hw-olist](https://github.com/dataengy/mlinside-hw-olist) (`demo/02-dagster`, история
сохранена) и подключён туда git-сабмодулем:

```bash
git submodule update --init demo/02-dagster
```
