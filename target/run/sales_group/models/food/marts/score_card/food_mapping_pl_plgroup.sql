
  
    

  create  table
    "food"."nghi_dev"."food_mapping_pl_plgroup__dbt_tmp"
    
    
    
  as (
    select 
'00' || gl_account as "G/L Account",
 name_gl_account, 
 pl_code, 
 pl_name, 
 pl_group_code, 
 pl_group_name
from "food"."nghi_dev"."food_seed_scorecard_mapping_pl_and_plgroup" pl
  );
  