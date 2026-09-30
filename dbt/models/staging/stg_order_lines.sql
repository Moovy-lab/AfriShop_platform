select
    order_line_id,
    order_id,
    product_id,
    seller_id,
    cast(quantity as integer)              as quantity,
    cast(unit_price as numeric(10,2))      as unit_price,
    cast(line_discount as numeric(10,2))   as line_discount,
    cast(line_total as numeric(12,2))      as line_total
from {{ source('curated', 'order_lines') }}