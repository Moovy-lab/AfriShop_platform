select
    cast(d as date)                  as date_day,
    extract(year from d)::int        as year,
    extract(month from d)::int       as month,
    to_char(d, 'YYYY-MM')            as year_month,
    extract(quarter from d)::int     as quarter,
    extract(isodow from d)::int      as day_of_week,
    extract(isodow from d) in (6, 7) as is_weekend
from generate_series('2025-01-01'::date, '2027-12-31'::date, interval '1 day') as d
