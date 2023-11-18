with
    ultimate as (
        select distinct brand_code
        from {{ ref("tech_qdtek_int_sales__material__distinct") }}
        where brand_code is not null
    )

select *
from ultimate
