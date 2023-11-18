{{
    config(
        materialized="table",
    )
}}


{{
    apply_logic_process_data_consolidates(
        "solomon", ref("qdt_stg_sol_log5001appxp_01610ab_accthist_erp_final"), '10'
    )
}}