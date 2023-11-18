select
    trim(source) as source,
    trim(salesgroup) as from_sales_group_name,
    trim(codesalesgroup) as to_sales_group_code

from {{ source("food_excel_sales", "md_sales_groups") }}
where source is not null
