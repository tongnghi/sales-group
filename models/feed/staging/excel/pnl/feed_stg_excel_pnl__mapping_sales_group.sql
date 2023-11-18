select 
    *
from {{ source("stg_excel_pnl", "mapping_sales_group") }}