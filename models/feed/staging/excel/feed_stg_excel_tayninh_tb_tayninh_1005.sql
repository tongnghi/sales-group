{{
    config(
        materialized="table",
    )
}}

select *
from {{ source("feed_excel_tayninh", "tb_tayninh_1005") }}
