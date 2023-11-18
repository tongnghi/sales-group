select trim(source) as source, trim(org_code) as from_name, trim(code) as to_code
from {{ ref("food_seed_sales_mapping_channels") }}
where source is not null
