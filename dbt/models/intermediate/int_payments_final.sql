-- Une ligne par commande : gère les retries (FAILED -> CAPTURED) et les paiements partiels
with ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by case when payment_status = 'CAPTURED' then 0 else 1 end,
                     payment_date desc
        ) as rn
    from {{ ref('stg_payments') }}
),

agg as (
    select
        order_id,
        count(*)                                                        as payment_attempts,
        sum(payment_amount) filter (where payment_status = 'CAPTURED')  as amount_captured,
        sum(payment_amount) filter (where payment_status = 'REFUNDED')  as amount_refunded,
        bool_or(payment_status = 'FAILED')                              as had_failed_attempt
    from {{ ref('stg_payments') }}
    group by order_id
)

select
    a.order_id,
    a.payment_attempts,
    a.amount_captured,
    a.amount_refunded,
    a.had_failed_attempt,
    r.payment_method  as final_payment_method,
    r.payment_status  as final_payment_status
from agg a
inner join ranked r
    on a.order_id = r.order_id and r.rn = 1
