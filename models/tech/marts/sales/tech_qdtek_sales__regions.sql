with ultimate as (select region as region_code, region_name from {{ ref("region") }})
select *
from ultimate
