select seller_id from {{ source('curated', 'products') }} where seller_id is not null
union
select seller_id from {{ source('curated', 'order_lines') }} where seller_id is not null
