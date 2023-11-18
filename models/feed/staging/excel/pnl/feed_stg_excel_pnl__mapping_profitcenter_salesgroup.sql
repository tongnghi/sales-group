select 
    profitcenter_1::varchar,
    profitcenter_2::varchar,
    sales_group_detail::varchar
from {{ source("stg_excel_pnl", "mapping_salesgroup_pnl") }}