{{ config(materialized="table") }}

select *
from {{ source("farm_redshift__prd__dwh", "f_actualsales") }}
where salesgroupid = '60' and trandatemonthid >= '202201'
