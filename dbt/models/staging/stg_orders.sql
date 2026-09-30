with ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by cast(updated_at as timestamptz) desc
        ) as rn
    from {{ source('curated', 'orders') }}
)

select
    order_id,
    customer_id,
    cast(order_date as timestamptz)        as order_date,
    order_status,
    channel,
    country_code,
    currency,
    cast(subtotal_amount as numeric(12,2)) as subtotal_amount,
    cast(shipping_amount as numeric(10,2)) as shipping_amount,
    cast(discount_amount as numeric(10,2)) as discount_amount,
    cast(total_amount as numeric(12,2))    as total_amount,
    cast(updated_at as timestamptz)        as updated_at
from ranked
where rn = 1
