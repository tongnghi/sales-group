{{
    config(
        materialized="table",
    )
}}

with latest_sales as (

    select 
        vbeln,
        kunnr,
        parvw,
        posnr,
        erdat,
        row_number() over (partition by vbeln, posnr, parvw order by aedat desc nulls last, erdat nulls last) as latest

    from {{ source("food_sap_s4", "tbl_vbpa") }}

)

select *
from latest_sales
where latest = 1