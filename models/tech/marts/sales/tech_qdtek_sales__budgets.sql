with
    ultimate as (
        select
            year,
            month,
            team_code,
            department_code,
            branch_code,
            region_code,
            sales_name,
            new_sales_code,
            old_sales_code,
            product_brand_name,

            case
                when upper(trim(product_brand_name)) = 'Khác'
                then 'OTHER'
                else upper(trim(product_brand_name))
            end as product_brand_code,

            product_group_name,
            upper(trim(product_group_name)) as product_group_code,
            "brand / product",

            case
                when "target amt m1/vnd" is null then 0 else "target amt m1/vnd"
            end as "target_amt_m1",

            case
                when "target amt m3/vnd" is null then 0 else "target amt m3/vnd"
            end as "target_amt_m3",

            case
                when "net amt /vnd" is null then 0 else "net amt /vnd"
            end as "net_amt",

            case when forecast is null then 0 else forecast end as forecast

        from {{ ref("tech_qdtek_stg_excel_sales__budget") }}
    )

select *
from ultimate
