with z_total as (

    select 
        *,
        case when qty = 0 and ending_qty = 0 then 0 
            else labour_cost + machinery_cost + electricity_cost + steam_cost + equipment_cost + overhead + subcontract_cost end as zero_qty 
    from {{ ref("feed_int_zmllistn__ztotal") }}

),

int_entry as (

    select
        plant,
        material,
        vendor,
        qty,
        qty * right(material,2) as qty_kg

    from {{ ref("feed_stg_sap_ecc__mb51") }} 
    where movement_type in ('101','102')

),

extra_entry as (

    select 
        z_total.plant, 
        z_total.material,
        z_total.qty,
        z_total.primary_material,
        z_total.packaging_material,
        z_total.labour_cost,
        z_total.machinery_cost,
        z_total.electricity_cost,
        z_total.steam_cost,
        z_total.equipment_cost,
        z_total.overhead,
        z_total.subcontract_cost
        
    from z_total
    left join int_entry using (plant, material)
    where int_entry.plant is null and z_total.zero_qty = 0

),

cal_entry as (

    select 
        int_entry.plant,
        int_entry.material,
        int_entry.qty_kg,
        z_total.primary_material * int_entry.qty_kg as primary_material,
        z_total.packaging_material * int_entry.qty_kg as packaging_material,
        z_total.labour_cost * int_entry.qty_kg as labour_cost,
        z_total.machinery_cost * int_entry.qty_kg as machinery_cost,
        z_total.electricity_cost * int_entry.qty_kg as electricity_cost,
        z_total.steam_cost * int_entry.qty_kg as steam_cost,
        z_total.equipment_cost * int_entry.qty_kg as equipment_cost,
        z_total.overhead * int_entry.qty_kg as overhead,
        z_total.subcontract_cost * int_entry.qty_kg as subcontract_cost

    from int_entry
    left join z_total on int_entry.vendor = z_total.plant and int_entry.material = z_total.material

),

final as (

    select 
        plant, 
        material,
        sum(qty_kg) as qty_kg,
        sum(primary_material) as primary_material,
        sum(packaging_material) as packaging_material,
        sum(labour_cost) as labour_cost,
        sum(machinery_cost) as machinery_cost,
        sum(electricity_cost) as electricity_cost,
        sum(steam_cost) as steam_cost,
        sum(equipment_cost) as equipment_cost,
        sum(overhead) as overhead,
        sum(subcontract_cost) as subcontract_cost

    from cal_entry
    group by plant, material
    union all
    select * from extra_entry

)

select * from final