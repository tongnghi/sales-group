with renamed as (
    select
        plant,
        division,
        "tỷ lệ" as tyle_var
    from {{ source("stg_excel_margin", "forecast_overhead_var") }}
),

cast_overhead as (
    select 
        plant::varchar,
        division::varchar,
        tyle_var::float,
        1-tyle_var as tyle_fix
    from renamed
)

select *
from cast_overhead