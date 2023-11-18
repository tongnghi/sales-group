with renamed as (

    select 
        werks as plant,
        lgort as sloc_code,
        txtmd as name
    from {{ source("food_sap_s4", "md_0stor_loc_text") }}

),

deduped as (

    {{
        dbt_utils.deduplicate(
            relation="renamed",
            partition_by="plant, sloc_code",
            order_by="sloc_code desc",
        )
    }}

)

select *
from deduped