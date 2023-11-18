
  
    

  create  table
    "food"."nghi_dev"."food_mapping_hierarchies_costelement__dbt_tmp"
    
    
    
  as (
    select
    hier_cost_element_code,
    hier_cost_element_name,
    case
        when cost_element is not null then right(cost_element, 10)
    end as cost_element_code
from "food"."nghi_dev"."food_seed_scorecard_hierarchies_costelement"
  );
  