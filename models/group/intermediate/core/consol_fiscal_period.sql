{{
    config(
        materialized="table",
    )
}}

select *
from {{ ref("period_input") }}
where period >= '2022001'