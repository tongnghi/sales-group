
  
    

  create  table
    "food"."nghi_dev"."food_stg_sap_s4__tbl_fcml_ccs_r_all_v__dbt_tmp"
    
    
    
  as (
    

with
    source as (select * from "food"."stg_sap_s4"."tbl_fcml_ccs_r_all_v"),

    deduped as (
        with row_numbered as (
        select
            _inner.*,
            row_number() over (
                partition by kalnr_mat, poper, bdatj, run_act, run_appl, categ, ptyp, psart, kalnr_pmat, bvalt, process, elesmhk, element, mlcct, curtp 
                order by kalnr_mat desc
            ) as rn
        from source as _inner
    )

    select
        distinct data.*
    from source as data
    
    natural join row_numbered
    where row_numbered.rn = 1
    )

select *
from deduped
  );
  