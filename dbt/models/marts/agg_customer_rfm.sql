{{
    config(
        materialized='incremental',
        unique_key=['customer_id', 'currency'],
        incremental_strategy='delete+insert'
    )
}}

-- Clients dont au moins une commande a changé depuis le dernier passage
with changed_customers as (
    select distinct customer_id
    from {{ ref('int_orders_enriched') }}
    {% if is_incremental() %}
    where updated_at > (select max(source_updated_at) from {{ this }})
    {% endif %}
)

-- Pas de somme entre devises (pas de taux de change) : une ligne par client et devise.
-- La récence n'est pas stockée : elle se calcule à la lecture (current_date - last_order_date).
select
    o.customer_id,
    o.currency,
    max(cast(o.order_date as date)) as last_order_date,
    count(*)                        as frequency,
    sum(o.total_amount)             as monetary,
    max(o.updated_at)               as source_updated_at
from {{ ref('int_orders_enriched') }} o
inner join changed_customers c on o.customer_id = c.customer_id
where o.order_status not in ('CANCELLED', 'PENDING')
group by o.customer_id, o.currency
