{{
    config(
        materialized="table",
    )
}}

select * from {{ source("farm_sol_arwhyapp", "xp_01610ab_accthist_erp_final") }}
