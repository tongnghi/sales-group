with
    farm_solomon as (select * from {{ ref("farm_int_sales_sol__added_farm_info") }}),

    farm_cammy as (select * from {{ ref("farm_int_sales_cammy__joined_dims") }}),

    unioned as (

        select *
        from farm_solomon

        union all

        select *
        from farm_cammy
    ),
    -- users don't want see the sales data not completed today
    exclude_today as (
        select *
        from unioned
        where trandate < trunc(convert_timezone('Asia/Ho_Chi_Minh', getdate()))
    )

select *
from exclude_today
