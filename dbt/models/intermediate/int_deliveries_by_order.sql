-- Une ligne par commande, avec le délai de livraison
with ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by shipped_at desc nulls last
        ) as rn
    from {{ ref('stg_deliveries') }}
)

select
    d.order_id,
    d.carrier,
    d.delivery_status,
    d.shipped_at,
    d.delivered_at,
    d.delivery_attempts,
    d.delivery_cost,
    cast(d.delivered_at as date) - cast(o.order_date as date)        as delivery_days,
    (cast(d.delivered_at as date) - cast(o.order_date as date)) >= 7 as is_late
from ranked d
inner join {{ ref('stg_orders') }} o
    on d.order_id = o.order_id
where d.rn = 1
