with ultimate as (select * from {{ ref("period") }})
select *
from ultimate
