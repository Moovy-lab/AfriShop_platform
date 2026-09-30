with versions as (
    select
        *,
        row_number() over (partition by customer_id order by dbt_valid_from) as version_no
    from {{ ref('snap_customer') }}
)

select
    dbt_scd_id                                   as customer_key,
    customer_id,
    email_hash,
    birth_year,
    gender,
    registration_date,
    country_code,
    city,
    customer_segment,
    loyalty_tier,
    account_status,
    case when version_no = 1 then cast('1900-01-01' as timestamp)
         else dbt_valid_from end                 as valid_from,
    coalesce(dbt_valid_to, cast('9999-12-31' as timestamp)) as valid_to,
    dbt_valid_to is null                         as is_current
from versions
