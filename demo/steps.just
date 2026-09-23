# СГЕНЕРИРОВАНО: python demo/tests/steps.py gen-just > demo/steps.just — не править руками.
# Проверки входа/результата каждого шага демо (demo/{N}-2-run.md, «Дано» / «Результат»).

# Демо 1 (Dagster с нуля), шаг D1-1 «Каркас и интерфейс»: проверка — вход
check-demo-1-step-1-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 1 1 input

# Демо 1 (Dagster с нуля), шаг D1-1 «Каркас и интерфейс»: проверка — результат
check-demo-1-step-1-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 1 1 result

# Демо 1 (Dagster с нуля), шаг D1-2 «Четыре ассета → граф»: проверка — вход
check-demo-1-step-2-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 1 2 input

# Демо 1 (Dagster с нуля), шаг D1-2 «Четыре ассета → граф»: проверка — результат
check-demo-1-step-2-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 1 2 result

# Демо 1 (Dagster с нуля), шаг D1-3 «Materialize all»: проверка — вход
check-demo-1-step-3-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 1 3 input

# Демо 1 (Dagster с нуля), шаг D1-3 «Materialize all»: проверка — результат
check-demo-1-step-3-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 1 3 result

# Демо 1 (Dagster с нуля), шаг D1-4 «Metadata»: проверка — вход
check-demo-1-step-4-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 1 4 input

# Демо 1 (Dagster с нуля), шаг D1-4 «Metadata»: проверка — результат
check-demo-1-step-4-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 1 4 result

# Демо 1 (Dagster с нуля), шаг D1-5 «Asset checks»: проверка — вход
check-demo-1-step-5-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 1 5 input

# Демо 1 (Dagster с нуля), шаг D1-5 «Asset checks»: проверка — результат
check-demo-1-step-5-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 1 5 result

# Демо 1 (Dagster с нуля), шаг D1-6 «Выборочный пересчёт»: проверка — вход
check-demo-1-step-6-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 1 6 input

# Демо 1 (Dagster с нуля), шаг D1-6 «Выборочный пересчёт»: проверка — результат
check-demo-1-step-6-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 1 6 result

# Демо 1 (Dagster с нуля), шаг D1-7 «Мостик ко второму демо»: проверка — вход
check-demo-1-step-7-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 1 7 input

# Демо 1 (Dagster с нуля), шаг D1-7 «Мостик ко второму демо»: проверка — результат
check-demo-1-step-7-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 1 7 result

# Демо 2 (dbt), шаг D2-1 «Откуда данные»: проверка — вход
check-demo-2-step-1-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 2 1 input

# Демо 2 (dbt), шаг D2-1 «Откуда данные»: проверка — результат
check-demo-2-step-1-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 2 1 result

# Демо 2 (dbt), шаг D2-2 «Из пустого проекта в готовый»: проверка — вход
check-demo-2-step-2-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 2 2 input

# Демо 2 (dbt), шаг D2-2 «Из пустого проекта в готовый»: проверка — результат
check-demo-2-step-2-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 2 2 result

# Демо 2 (dbt), шаг D2-3 «dbt одним компонентом»: проверка — вход
check-demo-2-step-3-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 2 3 input

# Демо 2 (dbt), шаг D2-3 «dbt одним компонентом»: проверка — результат
check-demo-2-step-3-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 2 3 result

# Демо 2 (dbt), шаг D2-4 «Модель dbt = ассет»: проверка — вход
check-demo-2-step-4-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 2 4 input

# Демо 2 (dbt), шаг D2-4 «Модель dbt = ассет»: проверка — результат
check-demo-2-step-4-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 2 4 result

# Демо 2 (dbt), шаг D2-5 «Тесты dbt = asset checks»: проверка — вход
check-demo-2-step-5-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 2 5 input

# Демо 2 (dbt), шаг D2-5 «Тесты dbt = asset checks»: проверка — результат
check-demo-2-step-5-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 2 5 result

# Демо 2 (dbt), шаг D2-6 «Зелёная модель, красный check»: проверка — вход
check-demo-2-step-6-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 2 6 input

# Демо 2 (dbt), шаг D2-6 «Зелёная модель, красный check»: проверка — результат
check-demo-2-step-6-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 2 6 result

# Демо 2 (dbt), шаг D2-7 «Freshness»: проверка — вход
check-demo-2-step-7-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 2 7 input

# Демо 2 (dbt), шаг D2-7 «Freshness»: проверка — результат
check-demo-2-step-7-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 2 7 result

# Демо 2 (dbt), шаг D2-8 «Резерв dbt-блока»: проверка — вход
check-demo-2-step-8-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 2 8 input

# Демо 2 (dbt), шаг D2-8 «Резерв dbt-блока»: проверка — результат
check-demo-2-step-8-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 2 8 result

# Демо 3 (CI + Observability), шаг D3-1 «CI: что именно зелёное»: проверка — вход
check-demo-3-step-1-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 3 1 input

# Демо 3 (CI + Observability), шаг D3-1 «CI: что именно зелёное»: проверка — результат
check-demo-3-step-1-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 3 1 result

# Демо 3 (CI + Observability), шаг D3-2 «Observability: run упал»: проверка — вход
check-demo-3-step-2-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 3 2 input

# Демо 3 (CI + Observability), шаг D3-2 «Observability: run упал»: проверка — результат
check-demo-3-step-2-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 3 2 result

# Демо 3 (CI + Observability), шаг D3-3 «Observability: run зелёный, check жёлтый»: проверка — вход
check-demo-3-step-3-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 3 3 input

# Демо 3 (CI + Observability), шаг D3-3 «Observability: run зелёный, check жёлтый»: проверка — результат
check-demo-3-step-3-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 3 3 result

# Демо 4 (ML), шаг D4-1 «Три набора строк»: проверка — вход
check-demo-4-step-1-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 1 input

# Демо 4 (ML), шаг D4-1 «Три набора строк»: проверка — результат
check-demo-4-step-1-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 1 result

# Демо 4 (ML), шаг D4-2 «Утечка»: проверка — вход
check-demo-4-step-2-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 2 input

# Демо 4 (ML), шаг D4-2 «Утечка»: проверка — результат
check-demo-4-step-2-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 2 result

# Демо 4 (ML), шаг D4-3 «Обучение»: проверка — вход
check-demo-4-step-3-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 3 input

# Демо 4 (ML), шаг D4-3 «Обучение»: проверка — результат
check-demo-4-step-3-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 3 result

# Демо 4 (ML), шаг D4-4 «Gate падает»: проверка — вход
check-demo-4-step-4-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 4 input

# Демо 4 (ML), шаг D4-4 «Gate падает»: проверка — результат
check-demo-4-step-4-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 4 result

# Демо 4 (ML), шаг D4-5 «Регистрация ≠ promotion»: проверка — вход
check-demo-4-step-5-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 5 input

# Демо 4 (ML), шаг D4-5 «Регистрация ≠ promotion»: проверка — результат
check-demo-4-step-5-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 5 result

# Демо 4 (ML), шаг D4-6 «Batch inference»: проверка — вход
check-demo-4-step-6-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 6 input

# Демо 4 (ML), шаг D4-6 «Batch inference»: проверка — результат
check-demo-4-step-6-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 6 result

# Демо 4 (ML), шаг D4-7 «Без champion»: проверка — вход
check-demo-4-step-7-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 7 input

# Демо 4 (ML), шаг D4-7 «Без champion»: проверка — результат
check-demo-4-step-7-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 7 result

# Демо 4 (ML), шаг D4-8 «Меняем SQL витрины»: проверка — вход
check-demo-4-step-8-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 8 input

# Демо 4 (ML), шаг D4-8 «Меняем SQL витрины»: проверка — результат
check-demo-4-step-8-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 8 result

# Демо 4 (ML), шаг D4-9 «Выборочный пересчёт»: проверка — вход
check-demo-4-step-9-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 9 input

# Демо 4 (ML), шаг D4-9 «Выборочный пересчёт»: проверка — результат
check-demo-4-step-9-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 9 result

# Демо 4 (ML), шаг D4-10 «Dev vs prod»: проверка — вход
check-demo-4-step-10-input:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 10 input

# Демо 4 (ML), шаг D4-10 «Dev vs prod»: проверка — результат
check-demo-4-step-10-result:
    cd "{{demo_root}}" && uv run python demo/tests/steps.py check 4 10 result

