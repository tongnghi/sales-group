select
    trim(source) as source,
    trim(salesgroup) as from_sales_group_name,
    trim(codesalesgroup) as to_sales_group_code
from {{ ref("food_seed_sales_mapping_lbc_sales_groups") }}
where source is not null
