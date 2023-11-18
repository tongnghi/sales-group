with
    transform as (
        select
            stt,
            region as region_name,
            industry as industry_code,
            dept as department_code,
            team as team_code,
            salescode as sales_code,
            staff as staff_name,
            title as title_name,
            trim(email) as email,
            report as accessing_report
        from {{ ref("pbi_user_permission") }}
    )

select *
from transform
