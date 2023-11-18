select 
    sales_group::varchar,
    sales_office::varchar
from {{ source("stg_excel_pnl", "mapping_salesgroup_ck") }}