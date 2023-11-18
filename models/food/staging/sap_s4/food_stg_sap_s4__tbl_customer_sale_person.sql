
with
    source as (
        select *
        from {{ source("food_sap_s4", "tbl_customer_sale_person") }}

    ),

    deduped as (
        {{
            dbt_utils.deduplicate(
                relation="source",
                partition_by="kunnr, vkorg, vtweg, spart, parvw, parza",
                order_by="kunnr desc",
            )
        }}
    )

select *
from deduped
