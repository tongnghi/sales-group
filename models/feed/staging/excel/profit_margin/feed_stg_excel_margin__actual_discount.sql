with renamed as (
    select 
        "week",
        "dis.channel" as dis_channel,
        "sale group" as sales_group,
        "product",
        "perpost",
        "sale office" as sales_office,
        "discount đ/kg" as discount_dkg
    from {{ source("stg_excel_margin", "actual_discount") }}
),

cast_discount as (
    
    select
        "week"::varchar,
        dis_channel::varchar,
        sales_group::varchar,
        "product"::varchar,
        lpad(replace(perpost::VARCHAR,'.',''),8,'0') as perpost,
        sales_office::varchar,
        discount_dkg::float
    from renamed
)

select * from cast_discount where perpost = '00082023'