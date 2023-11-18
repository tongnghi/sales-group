with total_ as (

    select 
        plant, 
        material, 
        cost_component_name,
        total_qty_expenditure,
        total_amt_expenditure,
        ending_qty, 
        ending_amt
    from {{ ref("feed_stg_sap_ecc__zmllistn") }}
    where row_type = 'Итог'

),

sum_qty as (

    select plant, material, sum(total_qty_expenditure) as qty, sum(ending_qty) as ending_qty
    from total_
    group by plant, material

),

total_ee as (

    select plant, material, cost_component_name, total_amt_expenditure from total_

),

pivot_total_ee as (

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
    from total_ee 
    pivot ( sum(total_amt_expenditure) for cost_component_name in ('Primary material','Packaging material','Labour cost', 'Machinery cost','Electricity cost','Steam cost','Equipment cost','Overhead','Subcontract cost') )
    
),

total_eb as (

    select plant, material, cost_component_name, ending_amt from total_

),

pivot_total_eb as (

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
    from total_eb
    pivot ( sum(ending_amt) for cost_component_name in ('Primary material','Packaging material','Labour cost', 'Machinery cost','Electricity cost','Steam cost','Equipment cost','Overhead','Subcontract cost') )
    
),

z_total as (
    select 
        sum_qty.plant,
        sum_qty.material,
        sum_qty.qty,
        sum_qty.ending_qty,

        case when sum_qty.qty != 0 then pivot_total_ee.primary_material / sum_qty.qty 
            else pivot_total_eb.primary_material / sum_qty.ending_qty end as primary_material,

        case when sum_qty.qty != 0 then pivot_total_ee.packaging_material / sum_qty.qty 
            else pivot_total_eb.packaging_material / sum_qty.ending_qty end as packaging_material,

        case when sum_qty.qty != 0 then pivot_total_ee.labour_cost / sum_qty.qty 
            else pivot_total_eb.labour_cost / sum_qty.ending_qty end as labour_cost,

        case when sum_qty.qty != 0 then pivot_total_ee.machinery_cost / sum_qty.qty 
            else pivot_total_eb.labour_cost / sum_qty.ending_qty end as machinery_cost,

        case when sum_qty.qty != 0 then pivot_total_ee.electricity_cost / sum_qty.qty 
            else pivot_total_eb.labour_cost / sum_qty.ending_qty end as electricity_cost,

        case when sum_qty.qty != 0 then pivot_total_ee.steam_cost / sum_qty.qty 
            else pivot_total_eb.labour_cost / sum_qty.ending_qty end as steam_cost,

        case when sum_qty.qty != 0 then pivot_total_ee.equipment_cost / sum_qty.qty 
            else pivot_total_eb.labour_cost / sum_qty.ending_qty end as equipment_cost,

        case when sum_qty.qty != 0 then pivot_total_ee.overhead / sum_qty.qty 
            else pivot_total_eb.labour_cost / sum_qty.ending_qty end as overhead,

        case when sum_qty.qty != 0 then pivot_total_ee.subcontract_cost / sum_qty.qty 
            else pivot_total_eb.labour_cost / sum_qty.ending_qty end as subcontract_cost

    from sum_qty
    left join pivot_total_ee on sum_qty.plant = pivot_total_ee.plant and sum_qty.material = pivot_total_ee.material
    left join pivot_total_eb on sum_qty.plant = pivot_total_eb.plant and sum_qty.material = pivot_total_eb.material
    {# where sum_qty.material = 'C1112AV1M025' and sum_qty.plant = '1010' #}

),

final as (
    
    select 
        plant, 
        material,
        qty,
        ending_qty,
        nvl(primary_material,0) as primary_material,
        nvl(packaging_material,0) as packaging_material,
        nvl(labour_cost,0) as labour_cost,
        nvl(machinery_cost,0) as machinery_cost,
        nvl(electricity_cost,0) as electricity_cost,
        nvl(steam_cost,0) as steam_cost, 
        nvl(equipment_cost,0) as equipment_cost,
        nvl(overhead,0) as overhead,
        nvl(subcontract_cost,0) as subcontract_cost

    from z_total

)

select * from final