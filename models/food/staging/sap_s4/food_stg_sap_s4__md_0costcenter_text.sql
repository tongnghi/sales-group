-- TODO: add dedup mechanism, ex. truncate before loading, or use macro (get lastest
-- row), ...
with
    selected_fields as (
        select kostl as code, txtsh as short_name, txtmd as medium_name
        from {{ source("food_sap_s4", "md_0costcenter_text") }}
        where langu = 'E' and kokrs = '1000'
    ),

    deduped as (
        {{
            dbt_utils.deduplicate(
                relation="selected_fields",
                partition_by="code",
                order_by="code asc",
            )
        }}
    )
select *
from deduped
order by code
