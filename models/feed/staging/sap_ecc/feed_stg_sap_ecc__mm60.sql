with mm60 as (

    select 
        *,
        row_number() over (partition by matnr order by erdat desc) as dedup

    from {{ source("feed_sap_ecc","mm60") }}

),

renamed as (

    select 
        werks as plant,
        matnr as material,
        ktext as material_description ,
        mtart as material_type ,
        matkl as material_group
    from mm60
    where dedup = 1

)

select * from renamed
