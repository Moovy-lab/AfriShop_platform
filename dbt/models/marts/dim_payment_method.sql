select
    payment_method,
    payment_method in ('OM_MOBILE_MONEY', 'WAVE', 'MTN_MOMO') as is_mobile_money
from (select distinct payment_method from {{ ref('stg_payments') }}) p
