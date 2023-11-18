{{
    config(
        materialized="table",
    )
}}

select
    code,
    '00' || gl_account_from as _gl_account_from,
    '00' || gl_account_to as _gl_account_to,
    excluded
from {{ ref("mapping_bs_gl_account") }}
