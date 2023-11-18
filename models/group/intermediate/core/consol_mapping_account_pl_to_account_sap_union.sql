{{
    config(
        materialized="table",
    )
}}

select
    company_code,
    'donavet'::text as sub_type,
    racct_donavet as racct_pl,
    racct_sap
from {{ ref('feed_stg_excel_donavet_tb_account_mapping') }}

union all

select
    company_code,
    'qdtek'::text as sub_type,
    racct_qdtek as racct_pl,
    racct_sap
from {{ ref('tech_stg_excel_qdtek_tb_account_mapping') }}

union all

select
    company_code,
    'lbc'::text as sub_type,
    racct_misa as racct_pl,
    racct_sap
from {{ ref('food_stg_excel_misa_lbc_tb_account_mapping') }}

union all

select
    company_code,
    'ntt'::text as sub_type,
    racct_qdtek as racct_pl,
    racct_sap
from {{ ref('tech_stg_excel_qdtek_tb_account_mapping') }}

