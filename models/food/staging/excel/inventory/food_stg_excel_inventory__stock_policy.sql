with renamed as (

    select 
        "mã vật tư" as material,
        "nhóm" as material_group,
        "mô tả" as description,
        "số ngày tk tối thiểu" as minimum,
        "số ngày tk tối đa" as maximum,
        row_number() over (partition by "mã vật tư") as first_  

    from {{ source("food_excel_inventory","stock_policy") }}

)

select * from renamed where first_ = 1