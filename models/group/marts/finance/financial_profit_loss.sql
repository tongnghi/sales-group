with

p_and_l as (
        
  {{ get_data_one_legal(ref("food_pl_3000_and_3100")) }}

    union all

  {{ get_data_one_legal(ref("farm_pl_1000_solomon")) }}

    union all

  {{ get_data_one_legal(ref("farm_pl_2000_solomon_dong_nam_bo")) }}

    union all

  {{ get_data_one_legal(ref("farm_pl_2000_solomon")) }}

    union all

  {{ get_data_one_legal(ref("farm_pl_2100_solomon")) }}

    union all
  
  {{ get_data_one_legal(ref("farm_pl_2200_1001_solomon")) }}

    union all

  {{ get_data_one_legal(ref("farm_pl_2200_2001_solomon")) }}

    union all
  
  {{ get_data_one_legal(ref("farm_pl_2200_2002_solomon")) }}

    union all
  
  {{ get_data_one_legal(ref("farm_pl_2300_solomon")) }}

    union all
  
  {{ get_data_one_legal(ref("farm_pl_5000_cam_solomon")) }}

    union all

  {{ get_data_one_legal(ref("farm_pl_5000_cbd1001_solomon")) }}

     union all
  
  {{ get_data_one_legal(ref("farm_pl_5000_star_solomon")) }}

    union all

  {{ get_data_one_legal(ref("farm_pl_5100_solomon")) }}

    union all

  {{ get_data_one_legal(ref("farm_pl_5200_solomon")) }}

    union all

  {{ get_data_one_legal(ref("feed_pl_1000_dnb_solomon")) }}

    union all
  
  {{ get_data_one_legal(ref("feed_pl_1000_sap")) }}

    union all
  
  {{ get_data_one_legal(ref("feed_pl_1100_donavet_excel")) }}

    union all
  
  {{ get_data_one_legal(ref("food_pl_3200_lbc")) }}

    union all
    
  {{ get_data_one_legal(ref("qdt_pl_4200_solomon")) }}

    union all
  
  {{ get_data_one_legal(ref("tech_pl_4001_qdtek_north")) }}

    union all
  
  {{ get_data_one_legal(ref("tech_pl_4002_qdtek_south")) }}

    union all
  
  {{ get_data_one_legal(ref("tech_pl_4300_ntt")) }}
    union all
  {{ get_data_one_legal(ref("feed_pl_1005_tayninh_excel")) }}


)
select
    legal,
    period,
    {# code, #}
    curtype,
    racct as gl_account,
    profit_center,
    sum(balance) balance
from p_and_l
group by legal
    , period
    , curtype
    , gl_account
    , profit_center