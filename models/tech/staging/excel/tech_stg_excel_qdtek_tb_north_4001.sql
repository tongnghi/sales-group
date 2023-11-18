{{
    config(
        materialized="table",
    )
}}

select * from {{ source("tech_excel_qdtek", "tb_qdtek_north") }}
