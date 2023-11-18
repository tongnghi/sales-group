{{
    config(
        materialized="table",
    )
}}


{# {{
    apply_logic_process_data_p_l(
        "excel", ref("feed_stg_excel_donavet_tb_donavet_1100"), '10','donavet'
    )
}} #}
with 
    raw_tb_1100_not_accum as (
        select  
        row_number() over (order by (select 1)) ::text as number_id,
        'donavet'::text as legal,
        period::text,
        '10'::text as curtype,
        '00' || _racct::text as racct,
        nvl(debit,0) - nvl(credit,0) as balance,
        ''::text as profit_center
        from {{ ref("feed_stg_excel_donavet_tb_donavet_1100") }} a
        where chi_tiet = 1 and left(_racct,1)  in ('5', '6', '7', '8')
    )

    select * 
    from raw_tb_1100_not_accum

    
