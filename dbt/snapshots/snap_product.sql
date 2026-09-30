{% snapshot snap_product %}
{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=['unit_price', 'unit_cost', 'product_status', 'category_name']
    )
}}
select * from {{ ref('stg_products') }}
{% endsnapshot %}
