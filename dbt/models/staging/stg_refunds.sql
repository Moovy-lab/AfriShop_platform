select
    payment_id,
    order_id,
    cast(payment_amount as numeric(12,2)) as refund_amount,
    currency,
    cast(payment_date as timestamptz)     as refunded_at
from {{ source('curated', 'payments') }}
where payment_status = 'REFUNDED'
