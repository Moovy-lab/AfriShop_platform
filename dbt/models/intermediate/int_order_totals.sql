-- Validation croisée : cohérence du total de l'en-tête, et écart avec la somme des lignes
with lines as (
    select
        order_id,
        count(*)        as line_count,
        sum(line_total) as lines_total
    from {{ ref('stg_order_lines') }}
    group by order_id
)

select
    o.order_id,
    l.line_count,
    l.lines_total,
    o.subtotal_amount,
    o.total_amount,
    o.subtotal_amount + o.shipping_amount - o.discount_amount as expected_total,
    abs(o.total_amount
        - (o.subtotal_amount + o.shipping_amount - o.discount_amount)) <= 1 as is_total_consistent,
    (l.lines_total is null)                                                 as has_no_lines,
    coalesce(abs(o.subtotal_amount - l.lines_total) > 1, false)             as is_subtotal_mismatch
from {{ ref('stg_orders') }} o
left join lines l
    on o.order_id = l.order_id
