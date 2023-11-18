{{
    config(
        materialized="table",
    )
}}

select
    "chi nhánh"::text as company_code,
    "account qdtek"::text as racct_qdtek,
    "account sap"::text as racct_sap
from {{ source("tech_excel_qdtek", "tb_account_mapping") }}
