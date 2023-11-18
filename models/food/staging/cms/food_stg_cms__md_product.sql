with renamed as (

    select 
        productcode as product_code, 
        productname as product_name, 
        sapproductcode as sap_product_code, 
        isactive as is_active,
        updatedat as updated_at,
        createdat as created_at,
        row_number() over (partition by sapproductcode order by updatedat desc) as latest
    from {{ source('food_cms_gkitchen', 'product') }}

)

select 
    * 
from renamed
where latest = 1