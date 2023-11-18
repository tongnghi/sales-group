select 
    account::varchar,
    description,
    pnl_code::varchar
from {{ source("stg_excel_pnl", "mapping_salesgroup_pnl") }}
