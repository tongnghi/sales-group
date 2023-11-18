{{
    config(
        materialized="table",
    )
}}


 {{
    apply_logic_process_data_consolidates(
        'sap_ecc', ref("feed_stg_sap_ecc__fi_0fi_gl_12"), '10'
    )
}}
