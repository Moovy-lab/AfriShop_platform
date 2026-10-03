select
    product_id,
    sku,
    product_name,
    category_name,
    subcategory_name,
    brand,
    seller_id,
    cast(unit_cost as numeric(10,2))       as unit_cost,
    cast(unit_price as numeric(10,2))      as unit_price,
    product_status
from {{ source('curated', 'products') }}
