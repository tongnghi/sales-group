with

tb_combination as (
        
{{ get_data_entity(ref("farm_tb_1000_solomon")) }}

        union all

{{ get_data_entity(ref("farm_tb_2000_solomon_dong_nam_bo")) }}

        union all
{{ get_data_entity(ref("farm_tb_2000_solomon")) }}

        union all
{{ get_data_entity(ref("farm_tb_2100_solomon")) }}

        union all
{{ get_data_entity(ref("farm_tb_2200_1001_solomon")) }}

        union all
{{ get_data_entity(ref("farm_tb_2200_2001_solomon")) }}

        union all
{{ get_data_entity(ref("farm_tb_2200_2002_solomon")) }}
        
        union all
{{ get_data_entity(ref("farm_tb_2300_solomon")) }}

        union all

{{ get_data_entity(ref("farm_tb_5000_cam_solomon")) }}

        union all
{{ get_data_entity(ref("farm_tb_5000_star_solomon")) }}

        union all
{{ get_data_entity(ref("farm_tb_5000_cbd1001_solomon")) }}

        union all
{{ get_data_entity(ref("farm_tb_5100_solomon")) }}

        union all
{{ get_data_entity(ref("farm_tb_5200_solomon")) }}

        union all
{{ get_data_entity(ref("feed_tb_1000_dnb_solomon")) }}

        union all
{{ get_data_entity(ref("feed_tb_1000_sap")) }}

        union all 
{{ get_data_entity(ref("feed_tb_1100_donavet_excel")) }}  

        union all 
{{ get_data_entity(ref("food_tb_3200_lbc")) }} 

        union all 
{{ get_data_entity(ref("food_tb_3000_and_3100")) }}

        union all
{{ get_data_entity(ref("qdt_tb_4200_solomon")) }} 

        union all
{{ get_data_entity(ref("tech_tb_4001_qdtek_north")) }} 

        union all
{{ get_data_entity(ref("tech_tb_4002_qdtek_south")) }}

         union all
{{ get_data_entity(ref("tech_tb_4300_ntt")) }}  
        
        union all
{{ get_data_entity(ref("feed_tb_1005_tayninh_excel")) }}  
    )
    
select
    legal,
    period,
    {# code, #}
    curtype,
    racct as gl_account,
    profit_center,
    sum(balance) balance
from tb_combination
group by legal
    , period
    , curtype
    , gl_account
    , profit_center
