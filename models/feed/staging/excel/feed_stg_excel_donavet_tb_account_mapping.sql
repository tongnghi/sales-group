{{
    config(
        materialized="table",
    )
}}

select
    "account donavet"::text as racct_donavet,
    "company code"::text as company_code,
    "account sap"::text as racct_sap
from {{ source("feed_excel_donavet", "tb_account_mapping") }}
