{{
    config(
        materialized="table",
    )
}}

select *
from {{ source("qdt_sol_log5001app", "xp_01610ab_accthist_erp_final") }}
