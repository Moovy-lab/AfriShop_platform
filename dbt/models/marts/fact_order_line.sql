select
    l.order_line_id,
    l.order_id,
    l.product_id,
    p.product_key,
    o.customer_id,
    c.customer_key,
    cast(o.order_date as date)  as order_date,
    o.order_status,
    o.channel,
    o.country_code,
    o.currency,
    l.quantity,
    l.unit_price,
    l.line_discount,
    l.line_total
from {{ ref('stg_order_lines') }} l
inner join {{ ref('stg_orders') }} o
    on l.order_id = o.order_id
left join {{ ref('dim_customer') }} c
    on o.customer_id = c.customer_id
   and cast(o.order_date as timestamp) >= c.valid_from
   and cast(o.order_date as timestamp) <  c.valid_to
left join {{ ref('dim_product') }} p
    on l.product_id = p.product_id
   and cast(o.order_date as timestamp) >= p.valid_from
   and cast(o.order_date as timestamp) <  p.valid_to
