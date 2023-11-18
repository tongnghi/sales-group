select 
    '00' || gl_manpower_cost as gl_manpower_cost
from {{ ref("food_seed_scorecard_mapping_kpis") }}