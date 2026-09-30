select customer_id, currency, count(*) as n
from {{ ref('agg_customer_rfm') }}
group by 1, 2
having count(*) > 1
