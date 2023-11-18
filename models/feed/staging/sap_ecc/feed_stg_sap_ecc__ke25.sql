with ke25 as (
    
    select 
        *,
        row_number() over (partition by perio, paledger, vrgar, versi, paobjnr, pasubnr, belnr, perbl) as dedup

    from {{ source("feed_sap_ecc","ke25") }}

),

renamed as (

    select 
        perio,
        ww005 as sales_manager,
        werks as plant,
        vtweg as dis_channel,
        vkorg as sales_org,
        vkgrp as sales_group,
        vkbur as sales_office,
        spart as division,
        artnr  as material,
        vv001 as Billed_Qty,
        vv310 as Primaterial_cost,
        vv320 as packaging_cost,
        vv330 as labor_cost,
        vv340 as machinery_cost,
        vv350 as electricity_cost,
        vv360 as steam_cost,
        vv370 as equipment_cost,
        vv380 as overhead

    from ke25
    where dedup = 1

)

select * from renamed where perio = '00092023'

