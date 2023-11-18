with ultimate as (select * from {{ ref("tech_qdtek_int_sales__salesman__distinct") }})

select *
from ultimate
