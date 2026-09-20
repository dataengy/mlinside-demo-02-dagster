{#
    Сегментация суммы по корзинам. Прямой наследник amount_bucket из ДЗ
    по фрод-транзакциям, пересчитанный под бразильские реалы: медианная цена
    позиции в Olist ≈ 75 R$, поэтому границы ниже, чем были для транзакций в USD.

    Зачем макрос, а не CASE в модели: та же логика нужна и в staging позиций,
    и в витрине по категориям. Продублируешь — рано или поздно границы разъедутся.

    Использование:
        {{ price_bucket('price') }} as price_segment
#}

{% macro price_bucket(amount) %}
    case
        when {{ amount }} is null then 'UNKNOWN'
        when {{ amount }} < 30 then 'LOW'
        when {{ amount }} < 100 then 'MEDIUM'
        when {{ amount }} < 300 then 'HIGH'
        else 'VERY_HIGH'
    end
{% endmacro %}
