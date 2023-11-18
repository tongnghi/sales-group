{{
    config(
        materialized="table",
    )
}}


{# {{
    apply_logic_process_data_consolidates(
        "excel", ref("food_stg_excel_misa_lbc_tp_lbc_3200_v2"), '10','lbc'
    )
}} #}

with 
    raw_pl_3200_not_accum as (
        select   
            row_number() over (order by (select 1)) ::text as number_id,
            'lbc'::text as legal,
            period::text,
            '10'::text as curtype,
            _racct::text as racct,
            nvl(debit,0) - nvl(credit,0) as balance,
            ''::text as profit_center
        from {{ ref("food_stg_excel_misa_lbc_tp_lbc_3200_v2") }} a
        where left(_racct,1) in ('5', '6', '7', '8')
    )
select * from raw_pl_3200_not_accum

