select 
    trim(code) as code,
    quy_doi as conversion_rate,
    trim(unit) as unit
from {{ source("food_excel_sales","md_products_conversion_rate_lbc") }}