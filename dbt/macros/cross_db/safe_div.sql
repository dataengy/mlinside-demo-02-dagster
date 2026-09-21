{#
    Деление, устойчивое к нулевому знаменателю.

    Расхождение тихое и оттого неприятное: ClickHouse на 0/0 вернёт nan,
    а на x/0 — inf, и это молча просочится в витрину. DuckDB в тех же случаях
    ведёт себя иначе. Явный NULL честнее: «доля не определена, потому что
    делить было не на что».

    Пример, где это срабатывает по-настоящему: штат, в котором ни один заказ
    ещё не доставлен. Знаменатель late_delivery_rate равен нулю.

    Макрос не диспетчеризуется — выражение и так стандартное SQL.

    Использование:
        {{ safe_div(count_if('is_late_delivery = 1'), count_if('is_delivered = 1')) }}
#}

{% macro safe_div(numerator, denominator) %}
    case
        when ({{ denominator }}) = 0 then null
        else ({{ numerator }}) / ({{ denominator }})
    end
{% endmacro %}


{#
    То же самое, но сразу в процентах и с округлением — так выглядит
    большинство *_rate колонок в витринах.

        {{ pct(count_if('is_late_delivery = 1'), count_if('is_delivered = 1')) }} as late_delivery_rate
#}
{% macro pct(numerator, denominator, digits=2) %}
    round(
        case
            when ({{ denominator }}) = 0 then null
            else 100.0 * ({{ numerator }}) / ({{ denominator }})
        end,
        {{ digits }}
    )
{% endmacro %}
