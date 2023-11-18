with dims as (

    select
        fiscyearper,
        material,
        sum(volume) as volume

    from {{ ref('food_int_fi_0fi_acdoca__calculated_kpis') }}
    group by fiscyearper, material

),

cogs_unit as (

    select 
        material,
        fiscyearper,
        cogs_sub_type, 
        cogs_type,
        cogs_value,
        cogs_unit_ea,
        cogs_unit_kg as cogs_unit_kg_z8

    from {{ ref('food_int_fi_0fi_acdoca__cogs') }}
    where cogs_type is not null and plant = '3101'

),

inventory_price as (

    select 
        matnr, 
        material, 
        fiscyearper, 
        gross_weight, 
        'Materials' as cogs_sub_type,
        'Materials costs' as cogs_type,
        avg(price_unit_kg) as price_unit_kg
    from {{ ref('food_base_price') }}
    group by matnr, material, fiscyearper, gross_weight, cogs_sub_type, cogs_type

),

final as (

    select
        dims.*,
        coalesce(cogs_unit.cogs_sub_type, inventory_price.cogs_sub_type) as cogs_sub_type,
        coalesce(cogs_unit.cogs_type, inventory_price.cogs_type) as cogs_type,
        coalesce(cogs_unit.cogs_unit_kg_z8,inventory_price.price_unit_kg) as cogs_unit_kg,
        cogs_unit_kg * dims.volume as cogs_value,

        cogs_unit.cogs_unit_ea,
        cogs_unit.cogs_value as cogs_value_z8,
        case when dims.volume != 0 then abs(cogs_unit.cogs_value/dims.volume) else 0 end as cogs_unit_based_on_volume
        

    from dims
    left join cogs_unit using (material, fiscyearper)
    left join inventory_price on inventory_price.matnr = dims.material and inventory_price.fiscyearper = dims.fiscyearper

)

select 
    material,
    fiscyearper,
    cogs_sub_type,
    cogs_type,
    avg(volume) as volume,
    sum(cogs_unit_kg) as cogs_unit_kg,
    sum(cogs_value) as cogs_value

from final
group by grouping sets ((material, fiscyearper), (material, fiscyearper, cogs_sub_type, cogs_type))