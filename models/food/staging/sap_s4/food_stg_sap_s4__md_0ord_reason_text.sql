with selected_fields as (

    select
        spras as language, 
        augru as reason_code,
        bezei as name
    from {{ source("food_sap_s4", "md_0ord_reason_text") }}
    where spras = 'E'

),

deduped as (
    {{
        dbt_utils.deduplicate(
            relation="selected_fields",
            partition_by="language, reason_code",
            order_by="name asc",
        )
    }}
)

select *
from deduped