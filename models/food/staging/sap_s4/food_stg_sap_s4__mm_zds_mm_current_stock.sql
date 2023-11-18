
{{
    config(
        pre_hook="update {{ source('food_sap_s4', 'mm_zds_mm_current_stock') }} set _created_at = sysdate where _created_at is null",
        materialized="table",
    )
}}

with source as (

    select 
        *,
        to_char(_created_at, 'YYYYMM') as year_month,
        _created_at::date as created_at,
        row_number() over (partition by charg, matnr, werks, lgort, created_at order by _created_at desc) as dedup

    from {{ source("food_sap_s4", "mm_zds_mm_current_stock") }}
)
-- 409758
select 
    *
from source
where dedup = 1
