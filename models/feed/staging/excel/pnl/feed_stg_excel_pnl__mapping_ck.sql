select 
    division::varchar,
    type_cktt::numeric,
    sales_office::varchar,
    sales_group::varchar
from {{ source("stg_excel_pnl", "mapping_ck") }}