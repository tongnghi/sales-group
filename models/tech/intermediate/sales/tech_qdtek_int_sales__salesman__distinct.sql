with
    raw_data as (select * from {{ ref("tech_qdtek_stg_excel_sales__salesman") }}),

    ultimate as (
        select distinct
            team_code,
            department_code,
            branch_code,
            sales_name,
            case
                upper(trim(new_sales_code))
                when 'ADMIN'
                then 'TEK305'
                else upper(trim(new_sales_code))
            end as new_sales_code,
            case
                upper(trim(old_sales_code))
                when 'ADMIN'
                then 'TEK305'
                else upper(trim(old_sales_code))
            end as old_sales_code
        from raw_data
    )

select *
from ultimate
