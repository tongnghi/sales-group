{{
    config(
        materialized="table",
    )
}}

{{
    apply_logic_process_data_p_l(
        'solomon',ref("farm_stg_sol_fasiaapp_xp_01610ab_accthist_erp_final"), '10'
    )
}}
