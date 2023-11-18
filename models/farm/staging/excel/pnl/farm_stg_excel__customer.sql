select 
    *
from {{ source("farm_excel_pnl", "customer") }}