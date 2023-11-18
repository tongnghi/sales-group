select 
    trim(source) as source, 
    trim(org_code) as from_code,
    trim(code) as to_code
from {{ source("food_excel_sales", "md_channels") }}
where source is not null
