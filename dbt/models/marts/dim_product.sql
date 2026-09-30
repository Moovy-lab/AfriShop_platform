with versions as (
    select
        *,
        row_number() over (partition by product_id order by dbt_valid_from) as version_no
    from {{ ref('snap_product') }}
)

select
    dbt_scd_id                                   as product_key,
    product_id,
    sku,
    product_name,
    category_name,
    subcategory_name,
    brand,
    seller_id,
    unit_cost,
    unit_price,
    product_status,
    case when version_no = 1 then cast('1900-01-01' as timestamp)
         else dbt_valid_from end                 as valid_from,
    coalesce(dbt_valid_to, cast('9999-12-31' as timestamp)) as valid_to,
    dbt_valid_to is null                         as is_current
from versions
