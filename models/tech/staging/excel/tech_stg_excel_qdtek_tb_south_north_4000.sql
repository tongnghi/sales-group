{{
    config(
        materialized="table",
    )
}}

select * from {{ source("tech_excel_qdtek", "tb_south_north_4000") }}