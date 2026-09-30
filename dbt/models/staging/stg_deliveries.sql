select
    delivery_id,
    order_id,
    carrier,
    delivery_status,
    cast(nullif(shipped_at, '') as timestamptz)    as shipped_at,
    cast(nullif(delivered_at, '') as timestamptz)  as delivered_at,
    cast(delivery_attempts as integer)             as delivery_attempts,
    cast(delivery_cost as numeric(10,2))           as delivery_cost
from {{ source('curated', 'deliveries') }}
