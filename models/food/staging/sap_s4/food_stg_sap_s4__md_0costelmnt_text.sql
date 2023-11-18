with
    selected_fields as (
        select kstar as code, txtsh as short_name, txtmd as full_name
        from {{ source("food_sap_s4", "md_0costelmnt_text") }}
        where langu = 'E' and kokrs = '1000'
    ),
    deduped as (
        {{
            dbt_utils.deduplicate(
                relation="selected_fields",
                partition_by="code",
                order_by="short_name asc",
            )
        }}
    )
select *
from deduped
order by code
