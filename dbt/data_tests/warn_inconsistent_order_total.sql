{{ config(severity='warn') }}
select order_id from {{ ref('int_order_totals') }} where not is_total_consistent
