select
    branch as branch_code,
    "b/p" as "brand / product",
    "net amt /vnd",
    "product group" as product_group_name,
    region as region_code,
    month,
    year,
    "target amt m1/vnd",
    "target amt m3/vnd",
    "product brand" as product_brand_name,
    "new sales code" as new_sales_code,
    "sales code" as old_sales_code,
    forecast,
    team as team_code,
    dept as department_code,
    sales as sales_name
from {{ source("qdtek_excel_sales", "forecast_budget") }}
