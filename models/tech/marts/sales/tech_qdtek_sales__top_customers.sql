with ultimate as (select * from {{ ref("top_customer") }})
select *
from ultimate
