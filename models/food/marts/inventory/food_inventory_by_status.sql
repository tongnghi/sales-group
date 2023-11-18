with pricing_by_material as (

    select 
        matnr, 
        year_month,
        avg(base_price) as unit_price

    from {{ ref('food_base_price') }}
    group by matnr, year_month

)

select 
    at_date,
    ltrim(matnr,'0') as material,
    year_month,
    stk_status,
    stock,
    pricing_by_material.unit_price as unit_price,
    stock * pricing_by_material.unit_price as stock_value,
    mat.unit as base_uom,
    mat.net_weight

from {{ ref('food_int_inventory__distribute_stocks') }} by_date
left join {{ ref('food_stg_sap_s4__md_0material_attr') }} mat on mat.code = by_date.matnr
left join pricing_by_material using (matnr, year_month)