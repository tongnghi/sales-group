with production_ as (

    select 
        plant,
        material, 
        cost_component_name,
        qty_income,
        income_amt_by_type

    from {{ ref("feed_stg_sap_ecc__zmllistn") }}
    where row_type = 'Расшифровка Прих' and reciept_type = 'BF'

),

prd_qty as (
    
    select 
        plant,
        material,
        sum(qty_income) as qty_income
    from production_ 
    group by plant, material

),

prd_selected_fields as (

    select 
        plant,
        material, 
        cost_component_name,
        income_amt_by_type
    from production_

),

prd as (

    select 
        plant,
        material,
        -- if 0 then apply another formula
        coalesce("primary material", 0) as primary_material, 
        coalesce("packaging material", 0) as packaging_material,
        coalesce("labour cost", 0) as labour_cost,
        coalesce("machinery cost", 0) as machinery_cost,
        coalesce("electricity cost", 0) as electricity_cost,
        coalesce("steam cost", 0) as steam_cost,
        coalesce("equipment cost", 0) as equipment_cost,
        coalesce("overhead", 0) as overhead,
        coalesce("subcontract cost", 0) as subcontract_cost
    from prd_selected_fields
    pivot ( sum(income_amt_by_type) for cost_component_name in ('Primary material','Packaging material','Labour cost', 'Machinery cost','Electricity cost','Steam cost','Equipment cost','Overhead','Subcontract cost') )
    
),

stock as (

    select 
        plant,
        material,
        opening_stock,
        0 as primary_material,
        0 as packaging_material,
        0 as labour_cost,
        0 as machinery_cost,
        0 as electricity_cost,
        0 as steam_cost,
        0 as equipment_cost,
        0 as overhead,
        0 as subcontract_cost

    from {{ ref("feed_stg_sap_ecc__mb5b") }} 

),

z_total as (

    select * from {{ ref("feed_int_zmllistn__ztotal") }}

),

internal_entry as (

    select * from {{ ref("feed_int_zmllistn__calculate_internal_entry") }}

),

balance as (

    select 
        internal_entry.plant,
        internal_entry.material,
        nvl(prd_qty.qty_income,0) + nvl(stock.opening_stock, 0) + internal_entry.qty_kg as qty,
        (nvl(stock.primary_material,0) + nvl(prd.primary_material,0) + internal_entry.primary_material) / qty  as primary_material,
        (nvl(stock.packaging_material,0) + nvl(prd.packaging_material,0) + internal_entry.packaging_material) / qty as packaging_material,
        (nvl(stock.labour_cost,0) + nvl(prd.labour_cost,0) + internal_entry.labour_cost) / qty as labour_cost,
        (nvl(stock.machinery_cost,0) + nvl(prd.machinery_cost,0) + internal_entry.machinery_cost) / qty as machinery_cost,
        (nvl(stock.electricity_cost,0) + nvl(prd.electricity_cost,0) + internal_entry.electricity_cost) / qty as electricity_cost,
        (nvl(stock.steam_cost,0) + nvl(prd.steam_cost,0) + internal_entry.steam_cost) / qty as steam_cost,
        (nvl(stock.equipment_cost,0) + nvl(prd.equipment_cost,0) + internal_entry.equipment_cost) / qty as equipment_cost,
        (nvl(stock.overhead,0) + nvl(prd.overhead,0) + internal_entry.overhead) / qty as overhead,
        (nvl(stock.subcontract_cost,0) + nvl(prd.subcontract_cost,0) + internal_entry.subcontract_cost) / qty as subcontract_cost

    from internal_entry
    left join prd_qty on internal_entry.plant = prd_qty.plant and internal_entry.material = prd_qty.material
    left join stock on stock.plant = internal_entry.plant and stock.material = internal_entry.material
    left join prd on internal_entry.plant = prd.plant and internal_entry.material = prd.material

),

final as (

    select 
        z_total.plant,
        z_total.material,
        z_total.qty, --- ?????????????????
        coalesce(balance.primary_material,z_total.primary_material) as primary_material,
        coalesce(balance.packaging_material,z_total.packaging_material) as packaging_material,
        coalesce(balance.labour_cost,z_total.labour_cost) as labour_cost,
        coalesce(balance.machinery_cost,z_total.machinery_cost) as machinery_cost,
        coalesce(balance.electricity_cost,z_total.electricity_cost) as electricity_cost,
        coalesce(balance.steam_cost,z_total.steam_cost) as steam_cost,
        coalesce(balance.equipment_cost,z_total.equipment_cost) as equipment_cost,
        coalesce(balance.overhead,z_total.overhead) as overhead,
        coalesce(balance.subcontract_cost,z_total.subcontract_cost) as subcontract_cost,
        z_total.qty * coalesce(balance.primary_material,z_total.primary_material) as valued

    from z_total
    left join balance using (plant, material)

)

select * from final 