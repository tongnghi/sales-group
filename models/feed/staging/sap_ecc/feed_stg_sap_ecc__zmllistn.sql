with zmllistn as (
    
    select 
        *,
        row_number() over (partition by erdat, ernam, tabix, perio) as dedup

    from {{ source("feed_sap_ecc","zmllistn") }}

),

zmllistn_deduped as (

    select 
        *,
        row_number() over (partition by perio order by erdat desc) sync_latest

    from zmllistn where dedup = 1

),

dim_sync as (

    select perio, erdat from zmllistn_deduped where sync_latest = 1

),

renamed as (

    select 
        perio,
        strtyp as row_type,
        prix_typ as reciept_type,
        prix_typ_t as description,
        curtp as currency_type,
        waers as currency,
        elemt  as cost_component,
        txele as cost_component_name,
        egrup as cost_component_group,
        txgrp as ccompgroup,
        bukrs as company_code,
        bwkey as plant,
        matnr as material,
        mtart as material_type,
        matkl as material_group,
        meins as unit_measure,
        p_prixod as qty_income,
        p_prixod_sum as amt_income,
        nzapas as begining_qty,
        nzapas_sum as begining_amt,
        t_prixod as total_qty_income,
        t_prixod_sum as total_amt_income,
        t_rasxod as total_qty_expenditure,
        t_rasxod_sum as total_amt_expenditure,
        konzapas as ending_qty,
        konzapas_sum as ending_amt,
        p_prixod_sum as income_amt_by_type

    from dim_sync
    left join zmllistn_deduped using (perio, erdat)

)

select * from renamed 
{# where perio = '00072023' -- dùng cho Margin #}
where perio = '00082023' -- dùng cho PnL


