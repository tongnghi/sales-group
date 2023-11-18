with reserstock as (

    select
        matnr,
        werks,
        lgort,
        charg,
        sum(lfimg) as reserstk
    from {{ ref('food_stg_sap_s4__sd_2lis_12_vcitm') }}
    where erdat >= to_char(getdate() + interval '7 hour' - interval '31 day', 'YYYYMMDD')
        and wbsta != 'C'
    group by matnr, werks, lgort, charg

),

cal_stock as (

    select
        matnr,
        werks,
        lgort,
        charg,
        created_at as at_date,
        to_char(created_at, 'YYYYMM') as year_month,
        lfgja || '0' || lfmon as d_fiscper,
        case when charg != '' then clabs else labst end as unrestk, -- unrestricted stock
        case when charg != '' then cumlm else umlme end as tranfstk, -- stock in transfer
        case when charg != '' then cinsm else insme end as qistock, -- QI stock
        case when charg != '' then ceinm else einme end as restcstk, --retricted stock
        case when charg != '' then cspem else speme end as blockstk, -- blocked stock
        case when charg != '' then cretm else retme end as retunstk, -- return stock
        nvl(reserstock.reserstk, 0) as reserstk_current, -- reservation stock
        unrestk - reserstk_current as availstk, -- available stock

        row_number() over (partition by year_month, matnr, werks, lgort, charg order by created_at desc) AS end_month

    from {{ ref('food_stg_sap_s4__mm_zds_mm_current_stock') }} cr_state
    left join reserstock using (matnr, werks, lgort, charg)

)

select 
    matnr,
    ltrim(matnr,'0') as material,
    at_date,
    year_month,
    sum(unrestk) as unrestk,
    sum(tranfstk) as tranfstk,
    sum(qistock) as qistock,
    sum(restcstk) as restcstk,
    sum(blockstk) as blockstk,
    sum(retunstk) as retunstk,
    sum(reserstk_current) as reserstk,
    sum(availstk) as availstk

from cal_stock
where end_month =  1
group by matnr, at_date, year_month