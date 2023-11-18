with renamed as (

    select
        perpost,
        plant,
        "sale group" as sales_group,
        product,
        mnf_v,
        mnf_f

    from {{ source("stg_excel_margin", "actual_mnf") }}
    
),

cast_mnf as (

    select 
        lpad(replace(perpost::VARCHAR,'.',''),8,'0') as perpost,
        plant::varchar,
        sales_group::varchar,
        product::varchar,
        sum(mnf_v::float) as mnf_v,
        sum(mnf_f::float) as mnf_f
    from renamed
    group by perpost,plant,sales_group,product
)

select *
from cast_mnf
where perpost = '00082023' 