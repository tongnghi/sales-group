with
    ultimate as (
        select distinct product_group_code
        from {{ ref("tech_qdtek_int_sales__material__distinct") }}
    )

select *
from ultimate
