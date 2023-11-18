{{
    config(
        pre_hook="update {{ source('food_sap_s4', 'tbl_ztt_zsdc0016') }} set _created_at = sysdate where _created_at is null",
        materialized="table",
    )
}}

with zsdc16 as (

    select 
        *

    from {{ source("food_sap_s4", "tbl_ztt_zsdc0016") }}

)

select 
    * 
from zsdc16 
where _created_at >= (select max(_created_at) from zsdc16)