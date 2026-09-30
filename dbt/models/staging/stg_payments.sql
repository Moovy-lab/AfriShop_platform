select
    payment_id,
    order_id,
    payment_method,
    payment_provider,
    cast(payment_amount as numeric(12,2)) as payment_amount,
    currency,
    payment_status,
    cast(payment_date as timestamptz)     as payment_date
from {{ source('curated', 'payments') }}
