with
    ultimate as (
        select distinct department_code
        from {{ ref("tech_qdtek_int_sales__salesman__distinct") }}
    )

select *
from ultimate
