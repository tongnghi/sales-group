with renamed as (
    select 
        invtid as code,
        descr as descr,
        "type code" as type_code,
        "net weight" as net_weight,
        unit as unit,
        level1 as product_hierarchy_l1_name,
        level2 as product_hierarchy_l2_name,
        level3 as product_hierarchy_l3_name,
        level4 as product_hierarchy_l4_name,
        level5 as product_hierarchy_l5_name,
        level6 as product_hierarchy_l6_name,
        level7 as product_hierarchy_l7_name,
        "level 8" as product_hierarchy_l8_name

    from {{ source("food_excel_sales", "md_products") }}

)

select * from renamed
