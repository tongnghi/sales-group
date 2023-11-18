select
    trim(source) as source,
    trim("customer group") as from_customer_group_name,
    trim("code customer group") as to_customer_group_code,
    trim("customer group 1") as from_customer_group_1_name,
    trim("code customer group 1") as to_customer_group_1_code,
    trim("customer group 2 ") as from_customer_group_2_name,
    trim("code customer group 2") as to_customer_group_2_code
    
from {{ source("food_excel_sales", "md_customer_groups") }}
where source is not null
