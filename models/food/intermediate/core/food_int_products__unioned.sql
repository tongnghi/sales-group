with matr_text as (

    select * from {{ ref("food_stg_sap_s4__md_0material_text") }}

),

matr_type_text as (

    select * from {{ ref("food_stg_sap_s4__md_0matl_type_text") }}

),

md_0prod_hier_text as (

    select * from {{ ref("food_stg_sap_s4__md_0prod_hier_text") }}

),

gkitchen_products as (

    select
        ltrim(matr_attr.code,'0') as code,

        matr_text.name,
        matr_attr.type_code as type_code,
        matr_type_text.name as type_name,
        matr_attr.net_weight,
        matr_attr.unit,
        matr_attr.product_hierarchy_l1_code,

        case when matr_attr.product_hierarchy_l1_code is not null
        then (select name from md_0prod_hier_text where md_0prod_hier_text.code = matr_attr.product_hierarchy_l1_code) 
        end as product_hierarchy_l1_name,

        matr_attr.product_hierarchy_l2_code,
        case when matr_attr.product_hierarchy_l2_code is not null
        then (select name from md_0prod_hier_text where md_0prod_hier_text.code = matr_attr.product_hierarchy_l2_code) 
        end as product_hierarchy_l2_name,

        matr_attr.product_hierarchy_l3_code,
        case when matr_attr.product_hierarchy_l3_code is not null
        then (select name from md_0prod_hier_text where md_0prod_hier_text.code = matr_attr.product_hierarchy_l3_code) 
        end as product_hierarchy_l3_name,

        matr_attr.product_hierarchy_l4_code,
        case when matr_attr.product_hierarchy_l4_code is not null
        then (select name from md_0prod_hier_text where md_0prod_hier_text.code = matr_attr.product_hierarchy_l4_code) 
        end as product_hierarchy_l4_name,

        matr_attr.product_hierarchy_l5_code,
        case when matr_attr.product_hierarchy_l5_code is not null
        then (select name from md_0prod_hier_text where md_0prod_hier_text.code = matr_attr.product_hierarchy_l5_code) 
        end as product_hierarchy_l5_name,

        matr_attr.product_hierarchy_l6_code,
        case when matr_attr.product_hierarchy_l6_code is not null
        then (select name from md_0prod_hier_text where md_0prod_hier_text.code = matr_attr.product_hierarchy_l6_code) 
        end as product_hierarchy_l6_name,

        matr_attr.product_hierarchy_l7_code,
        case when matr_attr.product_hierarchy_l7_code is not null
        then (select name from md_0prod_hier_text where md_0prod_hier_text.code = matr_attr.product_hierarchy_l7_code) 
        end as product_hierarchy_l7_name,

        matr_attr.product_hierarchy_l8_code,
        case when matr_attr.product_hierarchy_l8_code is not null
        then (select name from md_0prod_hier_text where md_0prod_hier_text.code = matr_attr.product_hierarchy_l8_code) 
        end as product_hierarchy_l8_name,
        
        minimum_shelf_life, 
        total_shelf_life,
        material_group,
        'sap_s4' as from_source
        
    from {{ ref("food_stg_sap_s4__md_0material_attr") }} matr_attr
    left join matr_text on matr_attr.code = matr_text.code
    left join matr_type_text on matr_attr.type_code = matr_type_text.code

),

cms_leboucher_products as (

    select
        md_products.code,
        md_products.descr as name,
        md_products.type_code as type_code,
        matr_type_text.name as type_name,
        md_products.net_weight,
        md_products.unit,
        null as product_hierarchy_l1_code,
        md_products.product_hierarchy_l1_name,
        null as product_hierarchy_l2_code,
        md_products.product_hierarchy_l2_name,
        null as product_hierarchy_l3_code,
        md_products.product_hierarchy_l3_name,
        null as product_hierarchy_l4_code,
        md_products.product_hierarchy_l4_name,
        null as product_hierarchy_l5_code,
        md_products.product_hierarchy_l5_name,
        null as product_hierarchy_l6_code,
        md_products.product_hierarchy_l6_name,
        null as product_hierarchy_l7_code,
        md_products.product_hierarchy_l7_name,
        null as product_hierarchy_l8_code,
        md_products.product_hierarchy_l8_name,

        null as minimum_shelf_life,
        null as total_shelf_life,
        null as material_group,
        'excel_file' as from_source

    from {{ ref('food_stg_excel_sales__md_products') }} md_products
    left join matr_type_text on md_products.type_code = matr_type_text.code

),

unioned_products as (
    -- not duplicated
    select * from gkitchen_products
    union all
    select * from cms_leboucher_products

),

dedup as (

    select 
        *,
        row_number() over (partition by code order by from_source desc) as priority

    from unioned_products

)

select 
    code,
    name,
    type_code,
    type_name,
    net_weight,
    unit,
    product_hierarchy_l1_code,
    coalesce(product_hierarchy_l1_name, 'Other') as product_hierarchy_l1_name,
    product_hierarchy_l2_code,
    coalesce(product_hierarchy_l2_name, 'Other') as product_hierarchy_l2_name,
    product_hierarchy_l3_code,
    coalesce(product_hierarchy_l3_name, 'Other') as product_hierarchy_l3_name,
    product_hierarchy_l4_code,
    coalesce(product_hierarchy_l4_name, 'Other') as product_hierarchy_l4_name,
    product_hierarchy_l5_code,
    coalesce(product_hierarchy_l5_name, 'Other') as product_hierarchy_l5_name,
    product_hierarchy_l6_code,
    coalesce(product_hierarchy_l6_name, 'Other') as product_hierarchy_l6_name,
    product_hierarchy_l7_code,
    coalesce(product_hierarchy_l7_name, 'Other') as product_hierarchy_l7_name,
    product_hierarchy_l8_code,
    coalesce(product_hierarchy_l8_name, 'Other') as product_hierarchy_l8_name,
    from_source,
    right(product_hierarchy_l4_code, 2) as ph4_code,
    right(product_hierarchy_l5_code, 3) as ph5_code,
    minimum_shelf_life,
    total_shelf_life,
    material_group
    
from dedup
where priority = 1