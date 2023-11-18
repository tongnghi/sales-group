{{
    config(
        materialized="table",
    )
}}

select * from {{ source("farm_sol_gfcamapp1", "xp_01610ab_accthist_erp_final") }}