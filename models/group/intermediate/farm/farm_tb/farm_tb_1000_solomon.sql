{{
    config(
        materialized="table",
    )
}}
-- depends_on: {{ ref('consol_mapping_bs_gl_account') }}

{{
    apply_logic_process_data_consolidates(
        'solomon',ref("farm_stg_sol_gfvmmapp_xp_01610ab_accthist_erp_final"), '10'
    )
}}

