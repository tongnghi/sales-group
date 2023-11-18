select 
    sales_group,
    mid_costcenter::varchar
from {{ source("stg_excel_pnl", "mapping_midcostcenter_salesgroup") }}