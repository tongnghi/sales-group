with renamed as (

    select 
        plant::varchar as plant,
        region,
        nvl("special stock",'') as special_stock,
        coalesce("storage location",special_stock)::varchar as storage_location,
        "physical wh" as physical_wh,
        "decs sloc" as desc_sloc,
        management
          
    from {{ source("food_excel_inventory","mapping_wh") }}

)

select * from renamed