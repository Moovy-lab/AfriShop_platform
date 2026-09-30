with ranked as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by registration_date desc
        ) as rn
    from {{ source('curated', 'customers') }}
)

select
    customer_id,
    email_hash,
    cast(birth_year as integer)     as birth_year,
    gender,
    cast(registration_date as date) as registration_date,
    country_code,
    city,
    customer_segment,
    loyalty_tier,
    account_status
from ranked
where rn = 1
