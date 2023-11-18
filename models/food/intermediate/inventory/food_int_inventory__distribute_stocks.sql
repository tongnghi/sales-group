with reserstock as (

    select
        matnr,
        werks,
        lgort,
        charg,
        sum(lfimg) as reserstk,
        current_date as created_at

    from {{ ref('food_stg_sap_s4__sd_2lis_12_vcitm') }}
    where erdat >= to_char(getdate() + interval '7 hour' - interval '31 day', 'YYYYMMDD')
        and wbsta != 'C'
    group by matnr, werks, lgort, charg

),

distribution_stocks as (

    select * from {{ ref('food_stg_sap_s4__mm_zds_mm_current_stock') }}
),

dim_end_month as (

    select year_month, max(created_at) as end_month
    from distribution_stocks
    group by year_month

),

cal_stock as (

    select
        matnr,
        werks,
        lgort,
        charg,
        created_at as at_date,
        year_month,
        lfgja || '0' || lfmon as d_fiscper,
        case when charg != '' then clabs else labst end as unrestk, -- unrestricted stock
        case when charg != '' then cumlm else umlme end as tranfstk, -- stock in transfer
        case when charg != '' then cinsm else insme end as qistock, -- QI stock
        case when charg != '' then ceinm else einme end as restcstk, --retricted stock
        case when charg != '' then cspem else speme end as blockstk, -- blocked stock
        case when charg != '' then cretm else retme end as retunstk, -- return stock
        nvl(reserstock.reserstk, 0) as reserstk_current, -- reservation stock
        unrestk - reserstk_current as availstk -- available stock

    from distribution_stocks
    left join reserstock using (matnr, werks, lgort, charg, created_at)

),

group_matnr as (

    select 
        matnr,
        at_date,
        cal_stock.year_month,
        sum(unrestk) as unrestricted_stock,
        sum(tranfstk) as transfer_stock,
        sum(qistock) as qi_stock,
        sum(restcstk) as retricted_stock,
        sum(blockstk) as blocked_stock,
        sum(retunstk) as return_stock,
        sum(reserstk_current) as reservation_stock,
        sum(availstk) as available_stock

    from cal_stock
    inner join dim_end_month on dim_end_month.year_month = cal_stock.year_month and dim_end_month.end_month = cal_stock.at_date
    group by matnr, at_date, cal_stock.year_month

)

select 
    matnr,
    at_date,
    year_month,
    stk_status,
    stock

from group_matnr
unpivot (stock for stk_status in (unrestricted_stock , transfer_stock, qi_stock, retricted_stock, blocked_stock, return_stock, reservation_stock, available_stock))