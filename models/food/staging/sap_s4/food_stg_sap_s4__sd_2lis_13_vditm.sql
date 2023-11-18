
{{
    config(
        materialized="table",
    )
}}

with source as (

    select * from {{ source("food_sap_s4", "sd_2lis_13_vditm") }}

),

deduped as (
    {{
        dbt_utils.deduplicate(
            relation="source",
            partition_by="vbeln, posnr, fkdat",
            order_by="fkdat desc",
        )
    }}
)

select * from deduped
