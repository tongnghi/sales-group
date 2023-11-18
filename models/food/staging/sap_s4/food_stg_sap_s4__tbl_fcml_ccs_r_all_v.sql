{{
    config(
        materialized="table",
    )
}}

with
    source as (select * from {{ source("food_sap_s4", "tbl_fcml_ccs_r_all_v") }}),

    deduped as (
        {{
            dbt_utils.deduplicate(
                relation="source",
                partition_by="kalnr_mat, poper, bdatj, run_act, run_appl, categ, ptyp, psart, kalnr_pmat, bvalt, process, elesmhk, element, mlcct, curtp ",
                order_by="kalnr_mat desc",
            )
        }}
    )

select *
from deduped
