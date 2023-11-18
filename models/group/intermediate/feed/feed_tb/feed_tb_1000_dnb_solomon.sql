{{
    config(
        materialized="table",
    )
}}


{{
    apply_logic_process_data_consolidates(
        "solomon", ref("feed_stg_sol_gfdnbapp_cn_dnb_tb_1000"), '10'
    )
}}
