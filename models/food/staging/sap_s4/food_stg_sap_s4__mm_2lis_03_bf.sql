{{
    config(
        materialized="table",
    )
}}

with source_ as (
    
    select * from {{ source("food_sap_s4", "mm_2lis_03_bf") }}

),

deduped as (
    {{
        dbt_utils.deduplicate(
            relation="source_",
            partition_by="mblnr, zeile, mjahr, bwcounter",
            order_by="mblnr desc",
        )
    }}
)

select *
from deduped