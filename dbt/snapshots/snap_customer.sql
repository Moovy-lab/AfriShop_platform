{% snapshot snap_customer %}
{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['city', 'customer_segment', 'loyalty_tier', 'account_status']
    )
}}
select * from {{ ref('stg_customers') }}
{% endsnapshot %}
