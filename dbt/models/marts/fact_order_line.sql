select
    l.order_line_id,
    l.order_id,
    l.product_id,
    o.customer_id,
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