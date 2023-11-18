{{
    config(
        materialized="table",
    )
}}
with
    sap_channels as (
        select code, name from {{ ref("food_stg_sap_s4__md_0distr_chan_text") }}
    ),

    excel_channles as (
        select to_code as code, max(from_code) as from_code
        from {{ ref("food_stg_excel_sales__md_channels") }}
        group by to_code
    )

select
    nvl(sap_channels.code, excel_channles.code) as code,
    nvl(sap_channels.name, excel_channles.from_code) as name,
    case when sap_channels.code is not null then 'SAP' else 'EXCEL' end as _source
from sap_channels
full join excel_channles using (code)
order by code
