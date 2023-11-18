with
    ultimate as (
        select distinct branch_code
        from {{ ref("tech_qdtek_int_sales__salesman__distinct") }}
    )

select *
from ultimate
