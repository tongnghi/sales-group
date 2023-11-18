with cast_cogs as (
    select 
        lpad(replace(perpost::VARCHAR,'.',''),8,'0') as perpost,
        plant::varchar,
        product::varchar,
        shrink::float
    from {{ source("stg_excel_margin", "actual_cogs") }}
)

select *
from cast_cogs
where perpost = '00082023'