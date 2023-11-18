{{
    config(
        materialized="table",
    )
}}

with source as (

    select * from {{ source("food_sap_s4", "sd_2lis_12_vcitm") }}

),

deduped as (
    {{
        dbt_utils.deduplicate(
            relation="source",
            partition_by="vbeln, posnr, erdat",
            order_by="erdat desc",
        )
    }}
)

select *
from deduped