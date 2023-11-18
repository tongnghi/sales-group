{{ config(materialized="table") }}

select 'ZF2' as billing_type, *
from {{ ref("farm_int_sol_vsdwh_salesvolumned__filtered_to_sales") }}

union all

select 'ZF2' as billing_type, null as company_code, *
from {{ ref("farm_stg_excel__master_data_adj") }}

union all

select 'ZS1' as billing_type, null as company_code, *
from {{ ref("farm_stg_excel__master_data_rev") }}
