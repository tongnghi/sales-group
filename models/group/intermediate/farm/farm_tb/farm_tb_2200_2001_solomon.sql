{{
    config(
        materialized="table",
    )
}}



{{
    apply_logic_process_data_consolidates(
        'solomon',ref("farm_stg_sol_mtr2001app_xp_01610ab_accthist_erp_final"), '10'
    )
}}