# Итог всех демо

[README](README.md) · демо: [1](1-3-results.md) · [2](2-3-results.md) · [3](3-3-results.md) · [4](4-3-results.md) · **9-3-all-results**

## Навигация

- [R9-1. Итог всех демо](#r9-1)
- [R9-2. Чеклист результата всех демо](#r9-2)
- [R9-3. Если не укладываемся в тайминг](#r9-3)
- [R9-4. Appendix (не в кадре)](#r9-4)

---

<a id="r9-1"></a>
## R9-1. Итог всех демо

- **Главный поинт:** **Dagster соединяет dbt-модели и ML-артефакты в один asset graph. По нему видно, из чего
  получена модель, что проверено и какой версией рассчитаны предсказания.**
- **Шаги** (что проговорить):
  1. [демо 1](1-2-run.md): ассеты, граф из аргументов, metadata, checks, выборочный пересчёт;
  2. [демо 2](2-2-run.md): модели dbt = ассеты, тесты = checks, freshness как статус;
  3. [демо 3](3-2-run.md): CI = рецепты, один экран и один канал алертов;
  4. [демо 4](4-2-run.md): blocking gate, register ≠ promote, inference по alias с `model_version`, пересчёт через
     границу dbt → Python;
  5. намеренно не показали: ingestion, внутренности ML, Docker, полный observability-стек ([appendix](#r9-4)).
- **UI:**
  - [http://localhost:3000/assets/predictions?view=lineage](http://localhost:3000/assets/predictions?view=lineage) —
    финальный кадр: от `raw/*` до `predictions`.

📚 [`docs/overview.md` §6](../docs/overview.md#6-appendix-периодическое-обучение-и-постоянный-инференс-как-это-обычно-устроено)

<a id="r9-2"></a>
## R9-2. Чеклист результата всех демо

```bash
for n in 1 2 3 4; do python demo/tests/steps.py complete $n; done
```

```bash
just demo-1-check-complete            # демо 1: проект demo/olist_ml и его история
just demo-2-check-complete            # демо 2: витрина, checks, dbt/models
just demo-3-check-complete            # демо 3: красный и зелёные dq_job, dbt/models
just demo-4-check-complete            # демо 4: реестр + champion, predictions
```

- [ ] Все четыре чеклиста ✅ — *авто*
- [ ] Итог (R9-1) произнесён на финальном кадре lineage — вручную
- [ ] Общее время ≈ 13 + 12.5 + 3 + 14.5 = 43 мин (или по срезу R9-3) — вручную

<a id="r9-3"></a>
## R9-3. Если не укладываемся в тайминг

- Общий тайминг: демо 1 ≈ 13 + демо 2 ≈ 12.5 + демо 3 ≈ 3 + демо 4 ≈ 14.5 мин.
- Кандидаты на срез (решает автор):
  - freshness в [D2-7](2-2-run.md#d2-7) — −1 мин (одной репликой);
  - негативный сценарий [D2-6](2-2-run.md#d2-6) — −1.5 мин (скриншот вместо живого прогона);
  - [D4-7](4-2-run.md#d4-7) (без champion) — −0.5 мин (одной репликой).

<a id="r9-4"></a>
## R9-4. Appendix (не в кадре)

- **Варианты ingestion:** dlt из Kaggle ([черновики](../.claude/drafts/ingest), критерии
  [ADR-03](../docs/decisions.md#adr-03-ingestion--предпосылка-не-демо-dbt-seed-shipped-snapshot--demo-prepare-dlt--stretch)); `raw/*` как внешние assets/sources от другой команды; паритет контракта —
  [`docs/contracts/raw.md`](../docs/contracts/raw.md).
- **Evidently:** HTML-отчёт по `predictions` как расширенное демо ([ADR-16](../docs/decisions.md#adr-16-evidently--не-в-mvp)).
- **Compose core + Grafana-стек:** [черновики observability](../.claude/drafts/observability),
  [`docs/deploy/`](../docs/deploy/README.md) (после MVP, [ADR-12](../docs/decisions.md#adr-12-docker-compose-и-grafana-стек--вне-mvp-docs-и-расширенное-демо)).
- **Прод-контуры** ([overview §6](../docs/overview.md#6-appendix-периодическое-обучение-и-постоянный-инференс-как-это-обычно-устроено)):
  партиции `predictions`, backfill, `SIM_TODAY`; мониторинг, поздние метки; challenger/champion, temporal split;
  online serving, feature store, Kafka.
- **Открытые решения:** [ADR-04a](../docs/decisions.md#adr-04a-как-провал-dbt-теста-останавливает-ml-ветку--варианты), [ADR-07a](../docs/decisions.md#adr-07a-baseline-версия-к-блоку-scoring--варианты).
