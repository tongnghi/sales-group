with
    selected_fields as (
        select ktokd as code, txt30 as name
        from {{ source("food_sap_s4", "md_0accnt_grp_text") }}
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
