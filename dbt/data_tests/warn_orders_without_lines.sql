{{ config(severity='warn') }}
select order_id from {{ ref('int_order_totals') }} where has_no_lines
