with ultimate as (select * from {{ ref("tech_qdtek_int_sales__material__distinct") }})

select *
from ultimate
