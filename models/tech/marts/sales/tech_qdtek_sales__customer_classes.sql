with
    ultimate as (
        select *
        from {{ ref("tech_qdtek_int_sales__customer_classs__grouped_by_month") }}
    )

select *
from ultimate
