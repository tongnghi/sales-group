
  
    

  create  table
    "food"."nghi_dev"."food_mapping_costcenter_costcentergroup__dbt_tmp"
    
    
    
  as (
    select 
cost_center as costcenter_code, 
cost_center_name as costcenter_name, 
cc_group as costcenter_group_code, 
cc_group_name as  costcenter_group_name
from "food"."nghi_dev"."food_seed_scorecard_mapping_cost_center_ccgroup"
  );
  