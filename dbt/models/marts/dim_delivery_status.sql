select *
from (values
    ('PREPARING',             1, false),
    ('IN_TRANSIT',            2, false),
    ('OUT_FOR_DELIVERY',      3, false),
    ('DELIVERED',             4, true),
    ('FAILED',                5, true),
    ('RETURNED_TO_WAREHOUSE', 6, true)
) as t(delivery_status, status_order, is_final)
