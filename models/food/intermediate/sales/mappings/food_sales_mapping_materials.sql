select
    material as code,
    trim(txtmd) as name
from {{ ref("food_seed_sales_mapping_materials") }}
