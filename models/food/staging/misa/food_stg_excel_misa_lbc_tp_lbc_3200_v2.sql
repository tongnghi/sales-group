{{
    config(
        materialized="table",
    )
}}

select *
from {{ source("food_excel_misa_lbc", "tp_lbc_3200_v2") }}
