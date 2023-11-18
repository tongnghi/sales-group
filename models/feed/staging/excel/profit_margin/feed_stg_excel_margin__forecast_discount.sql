with renamed as (
    select 
        version,
        perpost,
        "dis.channel" as dis_channel,
        "sale group" as sales_group,
        "sale office" as sales_office,
        product as material,
        "discount đ/kg" as discount_dkg
    from {{ source("stg_excel_margin", "forecast_discount") }}
)

select * from renamed