-- TODO: add dedup mechanism, ex. truncate before loading, or use macro (get lastest
-- row), ...
with
    selected_fields as (
        select vtweg as code, vtext as name
        from {{ source("food_sap_s4", "md_0distr_chan_text") }}
        where spras = 'E'
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
