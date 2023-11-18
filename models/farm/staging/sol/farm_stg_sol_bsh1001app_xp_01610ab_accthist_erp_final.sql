{{
    config(
        materialized="table",
    )
}}

select * from {{ source("farm_sol_bsh1001app", "xp_01610ab_accthist_erp_final") }}