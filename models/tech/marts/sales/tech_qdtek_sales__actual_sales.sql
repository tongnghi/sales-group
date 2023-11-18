with
    ultimate as (select * from {{ ref("tech_qdtek_int_sales__actual_sales__joined") }})

select *
from ultimate
