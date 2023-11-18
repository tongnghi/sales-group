{{
    config(
        materialized="table",
    )
}}

select * from {{ source("tech_excel_ntt", "tb_ntt_4300") }}
where code_pl != 0
