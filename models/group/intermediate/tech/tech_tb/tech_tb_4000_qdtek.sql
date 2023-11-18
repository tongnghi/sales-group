{{
    config(
        materialized="table",
    )
}}



{{
    apply_logic_process_data_consolidates(
        "excel", ref("tech_stg_excel_qdtek_tb_south_north_4000"), '10','qdtek'
    )
}}

