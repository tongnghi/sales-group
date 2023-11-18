{{
    config(
        materialized="table",
    )
}}


with 
    raw_tb_non_accum as (
        select 
        row_number() over (order by (select 1)) ::text as number_id,
        'ntt'::text as legal,
        period::text,
        '10'::text as curtype,
        _racct::text as racct,
        nvl(debit,0) - nvl(credit,0) as balance,
        ''::text as profit_center
        from {{ ref("tech_stg_excel_ntt_tb_ntt_4300") }} a
        where code_pl != 0 and left(_racct,1) in ('5', '6', '7', '8')
    )
    select * from raw_tb_non_accum
    