select
    trim("SYSTEM") as system_product,
    "NHAN HANG" as brand_name,
    upper(trim("NHAN HANG")) as brand_code,
    "PRODUCT GROUP" as product_group_code
from {{ source("qdtek_excel_sales", "material") }}
