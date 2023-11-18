with renamed as (
    select
        "sales office" as sales_office, 
        uom, 
        "  amount" as amount, 
        "version" as ver, 
        perpost, 
        material, 
        per, 
        unit, 
        "dis.channel" as dis_channel
    from {{ source("stg_excel_margin", "forecast_base_price") }}
),
cast_baseprice as (
    select
        sales_office::varchar,
        uom::varchar,
        amount::float,
        ver::varchar, 
        perpost::varchar, 
        material::varchar, 
        per::varchar, 
        unit::varchar, 
        dis_channel::varchar
    from renamed
)
select *
from cast_baseprice