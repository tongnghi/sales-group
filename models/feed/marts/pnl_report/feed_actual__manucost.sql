with internal_ke24 as (

    select
        plant,
        material,
        customer,
        doc_number,
        billed_qty,
        primaterial_cost,
        packaging_cost,
        labor_cost,
        machinery_cost,
        electricity_cost,
        steam_cost,
        equipment_cost,
        overhead,
        subcontract

    from {{ ref("feed_stg_sap_ecc__ke24") }} 
    where sched_line_cat = 'Z3' and material like 'C%%'

),

zmllistn as (

    select
        plant,
        material,
        primary_material,
        packaging_material,
        labour_cost,
        machinery_cost,
        electricity_cost,
        steam_cost,
        equipment_cost,
        overhead,
        subcontract_cost

    from {{ ref("feed_int_zmllistn__ztotal") }}

),

ke24 as (

    select
        ke24.plant,
        ke24.material,
        case when ke24.sales_group = '140' and ke24.division in ('10','20','30') then ke24.sales_group||'_'||ke24.division
            when ke24.sales_group in ('100','120','130','150','190') and ke24.division = '30' then ke24.sales_group||'_'||ke24.division
            when ke24.sales_group = '180' and ke24.sales_office = '1024' and ke24.division = '50' and ke24.dis_channel = '20' and ke24.material_group like 'A%%' and ke24.material_group like 'B%%' then ke24.sales_group||'_'||'140'
            when ke24.sales_group = '180' and ke24.sales_office = '1049' and ke24.division = '50' and ke24.dis_channel = '20' and ke24.material_group like 'A%%' and ke24.material_group like 'B%%' then ke24.sales_group||'_'||'180'
            when ke24.sales_group = '180' and ke24.sales_office = '1049' and ke24.division = '50' and ke24.dis_channel = '10' and ke24.material_group like 'A%%' and ke24.material_group like 'B%%' and ke24.customer = '101657' then ke24.sales_group||'_'||'QD'
            else ke24.sales_group end as sales_group,
        
        case when internal_ke24.customer is not null then nvl(zmllistn.primary_material*ke24.billed_qty + zmllistn.packaging_material*ke24.billed_qty,(internal_ke24.primaterial_cost/internal_ke24.billed_qty + internal_ke24.packaging_cost/internal_ke24.billed_qty) * ke24.billed_qty)
            else (ke24.primaterial_cost + ke24.packaging_cost) end as "17",

        case when internal_ke24.customer is not null then nvl(zmllistn.labour_cost*ke24.billed_qty,(internal_ke24.labor_cost/internal_ke24.billed_qty)*ke24.billed_qty)
            else (ke24.labor_cost) end as "32",
            
        case when internal_ke24.customer is not null then nvl(zmllistn.machinery_cost*ke24.billed_qty,(internal_ke24.machinery_cost/internal_ke24.billed_qty)*ke24.billed_qty)
            else (ke24.machinery_cost) end as "33",

        case when internal_ke24.customer is not null then nvl(zmllistn.electricity_cost*ke24.billed_qty,(internal_ke24.electricity_cost/internal_ke24.billed_qty)*ke24.billed_qty)
            else (ke24.electricity_cost) end as "34",

        case when internal_ke24.customer is not null then nvl(zmllistn.steam_cost*ke24.billed_qty,(internal_ke24.steam_cost/internal_ke24.billed_qty)*ke24.billed_qty)
            else (ke24.steam_cost) end as "35",

        case when internal_ke24.customer is not null then nvl(zmllistn.equipment_cost*ke24.billed_qty,(internal_ke24.equipment_cost/internal_ke24.billed_qty)*ke24.billed_qty)
            else (ke24.equipment_cost) end as "36",
        
        case when internal_ke24.customer is not null then nvl(zmllistn.overhead*ke24.billed_qty,(internal_ke24.overhead/internal_ke24.billed_qty)*ke24.billed_qty)
            else (ke24.overhead) end as "37",

        case when internal_ke24.customer is not null then nvl(zmllistn.subcontract_cost*ke24.billed_qty,(internal_ke24.subcontract/internal_ke24.billed_qty)*ke24.billed_qty)
            else (ke24.subcontract) end as "38"

    from {{ ref("feed_stg_sap_ecc__ke24") }} ke24
    left join internal_ke24 on ke24.plant = internal_ke24.customer and ke24.material = internal_ke24.material and ke24.doc_number = internal_ke24.doc_number
    left join zmllistn on internal_ke24.customer = zmllistn.plant and internal_ke24.material = zmllistn.material

),

manu_cost as (
    
    select 
        sales_group,
        "17",
        "32",
        "33",
        "34",
        "35",
        "36",
        "37",
        "38"

    from  ke24

),

unpivot_manu as (

    select 
        sales_group,
        kpi,
        manucost::numeric
        
    from manu_cost
    unpivot( manucost for kpi in ("17","32", "33", "34", "35", "36", "37","38"))

)

    select 
        sales_group, 
        kpi,
        sum(manucost) as manufactoring_cost
    from unpivot_manu 
    group by sales_group,kpi



