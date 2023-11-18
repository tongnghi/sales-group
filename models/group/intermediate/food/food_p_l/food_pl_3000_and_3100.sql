{{
    config(
        materialized="table",
    )
}}

{{
    apply_logic_process_data_p_l(
        "sap_s4", ref("food_stg_sap_s4__fi_0fi_gl_12"), '10'
    )
}}