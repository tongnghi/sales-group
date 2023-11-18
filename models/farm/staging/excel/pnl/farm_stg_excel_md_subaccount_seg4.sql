select 
    * 
from {{ source("farm_excel_pnl", "md_subaccount_seg4") }}
