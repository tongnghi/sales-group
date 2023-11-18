{{
    config(
        materialized="table",
    )
}}
with source as 
(
select
    cc::text as company_code,
    a::decimal(10,2) as "gl_asset", --tai san
    b::decimal(10,2) as "gl_giabilities", --no phai tra,
    c::decimal(10,2) as "gl_equity", --von
    d::decimal(10,2) as "gl_profit_ly", --loi nhuan nam truoc
    f::decimal(10,2) as "gl_profit_cy",--loi nhuan hien tai
    trim(vnd) as "currency",
    right(thang,4) || left(thang,3) as period

from {{ source("group_excel_financial_statements", "md_rate") }}
),


deduped as (
    {{
        dbt_utils.deduplicate(
            relation="source",
            partition_by="company_code, currency, period",
            order_by="company_code desc",
        )
    }}
)

select * from deduped
