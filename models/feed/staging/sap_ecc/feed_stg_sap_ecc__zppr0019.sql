with zppr19 as (

    select
        *,
        row_number() over (partition by erdat, ernam, tabix, perio, o_option, werks, aufnr) as dedup

    from {{ source("feed_sap_ecc","zppr0019") }}

),

period_latest as (

    select perio, max(erdat) as added_date
    from zppr19
    group by perio

),

deduped as (

    select 
        zppr19.*

    from period_latest
    left join zppr19 on zppr19.perio = period_latest.perio and zppr19.erdat = period_latest.added_date
    where zppr19.dedup = 1
    
),
 
renamed as (

    select 
        perio,
        werks as plant,
        ma_tpham as material,
        ma_tphan as ma_thanh_phan,
        slyc_kh as sanluongyeucau_KH,
        slyc_nk as sanluongyeucau_NK,
        slxn_tt as sanluongxacnhan_DVcoban,
        gt_xacnhan as giatrixacnhan,
        left(ma_tpham,1) as productlevel_1,
        left(ma_tpham,2) as productlevel_2,
        left(ma_tphan,1) as subproductlevel

    from deduped

)

select * from renamed where perio = '00082023'

