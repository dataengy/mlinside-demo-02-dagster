{#
    Отзывы — самая грязная таблица датасета, и дедупликация здесь обязательна.

    Что не так с сырыми данными:
      * 814 строк дублируют review_id;
      * у 547 заказов отзывов больше одного.

    Если не схлопнуть до одного отзыва на заказ, соединение с заказами размножит
    строки, и все метрики «доля плохих отзывов» окажутся смещены в пользу
    заказов, которые прокомментировали дважды.

    Оставляем последний отзыв по заказу: он отражает итоговое мнение покупателя.
    Сортировка добита review_id, чтобы результат был детерминированным при
    совпадающих отметках времени — иначе повторная сборка может дать другой ответ.
#}

with source as (
    select * from {{ source('olist_raw', 'order_reviews') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by order_id
            order by review_answer_timestamp desc, review_id asc
        ) as _row_number
    from source
)

select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,

    case when review_score <= 2 then 1 else 0 end as is_bad_review,
    case when review_score >= 4 then 1 else 0 end as is_good_review,

    -- Отзыв с текстом — сигнал вовлечённости: их пишут заметно реже, чем ставят оценку
    case
        when review_comment_message is not null and review_comment_message <> '' then 1
        else 0
    end as has_comment

from deduplicated
where _row_number = 1
