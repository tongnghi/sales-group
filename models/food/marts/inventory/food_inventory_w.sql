{# {{
    config(
        materialized="incremental",
        unique_key=["date_time", "matnr", "batch_id", "company_code", "storage_location", "plant"],
        incremental_strategy="delete+insert",
    )
}} #}

with stk_opening as (
    
    select 
        budat, 
        matnr, 
        werks,
        charg,
        bukrs,
        lgort,
        sum(issues_stock) as issues_stock,
        sum(reciept_stock) as reciept_stock,
        sum(issues_value) as issues_value,
        sum(reciept_value) as reciept_value,
        sum(prevs_val_inflow) as prevs_val_inflow,
        sum(pisvs_val_outflow) as pisvs_val_outflow,
        sum(pretotstk_inflow) as pretotstk_inflow,
        sum(pistotstk_outflow) as pistotstk_outflow,
        sum(sub_value) as sub_value,
        sum(sub_stock) as sub_stock

    from {{ ref("food_int_inventory__openning") }}
    group by budat, matnr, lgort, werks, charg, bukrs

),

distinct_matnr as (
    -- can limit dimension by filter (date-time and interval time) or active material
    -- warning have to accumulated befor limit date-time
    select 
        distinct matnr, lgort, werks, charg, bukrs
    from stk_opening

),

d_dates as (

    select date_day, calday 
    from {{ ref("dates") }}
    where calday >= '20211230' and calday <= to_char(getdate() + interval '7 hour', 'YYYYMMDD')

),

group_dim as (
    
    select 
        d_dates.calday as budat,
        distinct_matnr.matnr,
        distinct_matnr.werks,
        distinct_matnr.charg,
        distinct_matnr.lgort,
        distinct_matnr.bukrs
        
    from d_dates
    cross join distinct_matnr

),

filled as (
    
    select 
        group_dim.*,

        nvl(prevs_val_inflow,0) as prevs_val_inflow,
        nvl(pisvs_val_outflow,0) as pisvs_val_outflow,
        nvl(pretotstk_inflow,0) as pretotstk_inflow,
        nvl(pistotstk_outflow,0) as pistotstk_outflow,
        nvl(issues_stock,0) as issues_stock,
        nvl(reciept_stock,0) as reciept_stock,
        nvl(issues_value,0) as issues_value,
        nvl(reciept_value,0) as reciept_value,
        nvl(sub_value,0) as sub_value,
        nvl(sub_stock,0) as sub_stock

    from group_dim
    left join stk_opening using (budat , matnr, werks, charg, bukrs, lgort)

),

accumulated as (

    select
        filled.*,
        sum(sub_value) over ( partition by matnr, werks, charg, bukrs, lgort order by budat rows unbounded preceding) as closing_value,
        sum(sub_stock) over ( partition by matnr, werks, charg, bukrs, lgort order by budat rows unbounded preceding) as closing_stock

    from filled

)


select 
    budat as date_time,
    left(budat, 6) as year_month,
    matnr,
    ltrim(matnr,'0') as material,
    charg as batch_id,
    bukrs as company_code,
    lgort as storage_location,
    werks as plant,

    issues_stock,
    reciept_stock,
    issues_value,
    reciept_value,

    lag(closing_stock, 1) over (partition by matnr, werks, charg, bukrs, lgort order by budat asc) as opening_stock,
    lag(closing_value, 1) over (partition by matnr, werks, charg, bukrs, lgort order by budat asc)  as opening_value,
    closing_stock,
    closing_value,
    pretotstk_inflow,
    pistotstk_outflow,
    
    prevs_val_inflow,
    pisvs_val_outflow
    

from accumulated