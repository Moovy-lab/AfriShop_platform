-- Une ligne par commande, avec paiement, livraison, cohérence du total et retour
select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_status,
    o.channel,
    o.country_code,
    o.currency,
    o.total_amount,
    t.line_count,
    t.lines_total,
    t.is_total_consistent,
    t.has_no_lines,
    t.is_subtotal_mismatch,
    p.payment_attempts,
    p.had_failed_attempt,
    p.amount_captured,
    p.amount_refunded,
    p.final_payment_method,
    (coalesce(p.amount_captured, 0) > 0
        and coalesce(p.amount_captured, 0) < o.total_amount)      as is_partially_paid,
    d.delivery_status,
    d.shipped_at,
    d.delivered_at,
    d.delivery_days,
    d.is_late,
    (o.order_status = 'RETURNED')                                  as is_returned,
    (coalesce(p.amount_refunded, 0) > 0)                           as has_refund,
    (d.delivery_status = 'RETURNED_TO_WAREHOUSE')                  as is_returned_to_warehouse
from {{ ref('stg_orders') }} o
left join {{ ref('int_order_totals') }}        t on o.order_id = t.order_id
left join {{ ref('int_payments_final') }}      p on o.order_id = p.order_id
left join {{ ref('int_deliveries_by_order') }} d on o.order_id = d.order_id
