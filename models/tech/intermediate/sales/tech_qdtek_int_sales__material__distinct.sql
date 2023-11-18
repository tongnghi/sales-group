with
    raw_data as (select * from {{ ref("tech_qdtek_stg_excel_sales__material") }}),

    ultimate as (
        select distinct
            system_product,
            brand_name,
            product_group_code,
            case brand_code when 'KHÁC' then 'OTHER' else brand_code end as brand_code,
            upper(system_product) as product_code
        from raw_data
    )

select *
from ultimate
