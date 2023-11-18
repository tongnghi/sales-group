with stk as (

    select
        date_time,
        matnr,
        plant,
        year_month,
        round(sum(closing_stock),2) as closing_stock,
        sum(closing_value) as closing_value
        
    from {{ ref('food_inventory_w') }}
    group by matnr, plant, date_time, year_month

),

recency_price as (

    select *,
       case when closing_stock > 1 or closing_stock < -1 then 1 else 0 end has_stk,
       row_number() over (partition by matnr, plant, year_month order by has_stk desc, date_time desc) as recently_
    from stk
),

final as (

    select
        matnr,
        ltrim(matnr,'0') as material,
        plant,
        year_month,
        date_time as at_date,
        case when closing_stock != 0 then abs(closing_value/closing_stock) end as base_price,
        coalesce(base_price, lag(base_price, 1) ignore nulls over (partition by matnr, plant order by year_month asc )) as forwardfill_price,
        coalesce(base_price, lag(base_price, 1) ignore nulls over (partition by matnr, plant order by year_month desc)) as backfill_price,
        coalesce(backfill_price, forwardfill_price) as price
    from recency_price
    where recently_ = 1

)

select
    matnr,
    material,
    plant,
    year_month,
    at_date,
    to_char(at_date::date, 'YYYY0MM') as fiscyearper,
    nvl(price, 0) as base_price,
    md_matnr.gross_weight,
    case when md_matnr.gross_weight != 0 then nvl(price, 0)/md_matnr.gross_weight else nvl(price, 0) end as price_unit_kg

from final
left join {{ ref('food_stg_sap_s4__md_0material_attr') }} md_matnr on md_matnr.code = final.matnr