-- TODO: add dedup mechanism, ex. truncate before loading, or use macro (get lastest
-- row), ...
with
    selected_fields as (
        select werks as code, txtmd as name
        from {{ source("food_sap_s4", "md_0plant_text") }}
    ),

    deduped as (
        {{
            dbt_utils.deduplicate(
                relation="selected_fields",
                partition_by="code",
                order_by="name asc",
            )
        }}
    )
select *
from deduped
order by code
