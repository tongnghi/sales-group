with ke24 as (
    
    select 
        *,
        row_number() over (partition by paledger, vrgar, versi, perio , paobjnr, pasubnr, rbeln, rposn, indx) as dedup

    from {{ source("feed_sap_ecc","ke24") }}

),

renamed as (

    select 
        perio,
        werks as plant,
        belnr as doc_number,
        vkorg as sales_org,
        to_date("budat",'YYYY-MM-DD',false) as posting_date,
        rbeln as ref_doc,
        rposn as ref_item_num,
        usnam as created_by,
        vkgrp as sales_group,
        vkbur as sales_office,
        ww005 as sales_manager,
        ww910 as sched_line_cat,
        wadat as goods_issue_date,
        vtweg as dis_channel,
        spart as division,
        kndnr as customer,
        artnr as material,
        matkl as material_group,
        mtart as material_type,
        paph9,
        vv001::numeric as billed_qty,
        vv100::numeric as factory_price,
        vv190::numeric as increasing_price,
        vv185::numeric as decreasing_price,
        vv105::numeric as discount_price,
        vv110::numeric as discount_price_d_kg,
        vv115::numeric as delivery_fee,
        vv150 as gia_hoa_don,
        vv180 as refund_price,
        vv205::numeric as cktt_sauhd,
        vv210::numeric as cktt_sauhd_d_kg,
        vv245 as thanhtoan_ck,
        vv250::numeric as discount_by_ratio,
        vv255::numeric as discount_by_d_kg,
        vv300 as standard_cogs,
        vv310 as primaterial_cost,
        vv320 as packaging_cost,
        vv330 as labor_cost,
        vv340 as machinery_cost,
        vv350 as electricity_cost,
        vv360 as steam_cost,
        vv370 as equipment_cost,
        vv380 as overhead,
        vv390 as subcontract

    from ke24
    where dedup = 1

)

select * from renamed where perio = '00082023'

