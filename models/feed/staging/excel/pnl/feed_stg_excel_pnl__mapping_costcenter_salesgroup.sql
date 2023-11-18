select 
    sales_group::varchar,
    cost_center::varchar
from {{ source("stg_excel_pnl", "mapping_costcenter_salesgroup") }}