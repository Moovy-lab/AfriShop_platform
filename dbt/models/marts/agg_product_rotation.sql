{{
    config(
        materialized='incremental',
        unique_key=['product_id', 'month_start', 'currency'],
        incremental_strategy='delete+insert'
    )
}}

select
    f.product_id,
    cast(date_trunc('month', f.order_date) as date) as month_start,
    f.currency,
    count(distinct f.order_id) as orders_count,
    sum(f.quantity)            as units_sold,
    sum(f.line_total)          as revenue
from {{ ref('fact_order_line') }} f
where f.product_key is not null
  and f.order_status not in ('CANCELLED', 'PENDING')
{% if is_incremental() %}
  and f.order_date >= (select coalesce(max(month_start), date '1900-01-01') - interval '1 month' from {{ this }})
{% endif %}
group by 1, 2, 3
