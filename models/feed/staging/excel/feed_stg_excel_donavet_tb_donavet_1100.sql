{{
    config(
        materialized="table",
    )
}}

select *
from {{ source("feed_excel_donavet", "tb_donavet_1100") }}
