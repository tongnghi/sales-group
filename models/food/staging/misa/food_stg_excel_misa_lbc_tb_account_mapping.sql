{{
    config(
        materialized="table",
    )
}}

select
    "account misa"::text as racct_misa,
    "company code"::text as company_code,
    left("sap account",8)::text as racct_sap
from {{ source("food_excel_misa_lbc", "tb_account_mapping") }}
