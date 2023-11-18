with
    ultimate as (
        select * from {{ ref("tech_qdtek_int_sales__churn__grouped_by_customer") }}
    )

select *
from ultimate
