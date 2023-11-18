{{
    config(
        materialized="table",
    )
}}

with um_03 as (

    select * from {{ source("food_sap_s4", "mm_2lis_03_um") }}

)
-- TODO: the group keys was wrong
{# deduped as (
    {{
        dbt_utils.deduplicate(
            relation="source",
            partition_by="budat, bukrs, matnr, bwart, werks, bwvorg, meins, lifnr, bwkey, belnr, bldat, sobkz, bklas, bwapplnm, bsttyp, bstaus",
            order_by="budat desc",
        )
    }}
) #}

select *
from um_03