{{ 
    config(materialized="table") 
}}

with selected_fields as (
    
    select
        matnr as code,
        ltrim(matnr,'0') as code_,
        mtart as type_code,
        matkl as material_group,
        meins as unit,
        gewei as weight_unit,
        brgew as gross_weight,
        ntgew as net_weight,
        case when len(prdha) >= 1 then left(prdha, 1) end as product_hierarchy_l1_code,
        case when len(prdha) >= 2 then left(prdha, 2) end as product_hierarchy_l2_code,
        case when len(prdha) >= 3 then left(prdha, 3) end as product_hierarchy_l3_code,
        case when len(prdha) >= 5 then left(prdha, 5) end as product_hierarchy_l4_code,
        case when len(prdha) >= 8 then left(prdha, 8) end as product_hierarchy_l5_code,
        case when len(prdha) >= 12 then left(prdha, 12) end as product_hierarchy_l6_code,
        case when len(prdha) >= 16 then left(prdha, 16) end as product_hierarchy_l7_code,
        case when len(prdha) >= 18 then left(prdha, 18) end as product_hierarchy_l8_code,
        mhdrz as minimum_shelf_life, 
        mhdhb as total_shelf_life,
        row_number() over (partition by matnr order by matnr asc) as dedup

    from {{ source("food_sap_s4", "md_0material_attr") }}

),

identify_latest as (

    select 
        *,
        max(dedup) over (partition by code) as row_latest

    from selected_fields

),
-- deduped wrong
{# deduped as (

    {{
        dbt_utils.deduplicate(
            relation="selected_fields",
            partition_by="code",
            order_by="code asc",
        )
    }}
) #}
-- Don't need order by
final as (

    select 
        *
    from identify_latest
    where dedup = row_latest
    order by code

)

select * from final
