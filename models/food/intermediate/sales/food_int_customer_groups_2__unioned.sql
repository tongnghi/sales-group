{{
    config(
        materialized="table",
    )
}}
with
    sap_groups as (
        select code, name from {{ ref("food_stg_sap_s4__md_0cust_grp2_text") }}
    ),

    lbc_groups as (
        select to_customer_group_2_code as code, max(from_customer_group_2_name) as name
        from {{ ref("food_stg_excel_sales__md_customer_groups") }}
        group by code
    )

select
    nvl(sap_groups.code, lbc_groups.code) as code,
    nvl(sap_groups.name, lbc_groups.name) as name,
    case when sap_groups.code is not null then 'SAP' else 'EXCEL' end as _source
from sap_groups
full join lbc_groups using (code)
order by code
