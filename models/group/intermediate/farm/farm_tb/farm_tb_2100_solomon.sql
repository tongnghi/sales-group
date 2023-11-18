{{
    config(
        materialized="table",
    )
}}


{{
    apply_logic_process_data_consolidates(
        'solomon',ref("farm_stg_sol_arwhyapp_xp_01610ab_accthist_erp_final"), '10'
    )
}}

