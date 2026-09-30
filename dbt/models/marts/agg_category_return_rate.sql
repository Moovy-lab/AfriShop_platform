{{
    config(
        materialized='incremental',
        unique_key=['category_name', 'month_start'],
        incremental_strategy='delete+insert'
    )
}}

select
    p.category_name,
    cast(date_trunc('month', f.order_date) as date) as month_start,
    count(*)                                        as lines_sold,
    count(*) filter (where o.is_returned)           as lines_returned,
    round(100.0 * count(*) filter (where o.is_returned) / count(*), 2) as return_rate_pct
from {{ ref('fact_order_line') }} f
inner join {{ ref('dim_product') }} p        on f.product_key = p.product_key
inner join {{ ref('int_orders_enriched') }} o on f.order_id = o.order_id
where f.order_status <> 'CANCELLED'
{% if is_incremental() %}
  and f.order_date >= (select coalesce(max(month_start), date '1900-01-01') - interval '1 month' from {{ this }})
{% endif %}
group by 1, 2
