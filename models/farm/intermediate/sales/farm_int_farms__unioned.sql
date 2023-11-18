select
    farmid::varchar,
    farmname,
    ftype,
    buid::varchar,
    buname,
    sub,
    regionid::varchar,
    case when compcode is null then '9999' else compcode::varchar end as compcode
from {{ ref("farm_stg_excel__master_farms") }}
where sub != '0'

union all

select
    farmid,
    farmname,
    ftype,
    buid::character varying(100),
    buname,
    sub,
    regionid::character varying(100),
    compcode::character varying(100)
from {{ ref("farm_seed_sales_manual_farms") }}
