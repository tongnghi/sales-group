{{
    config(
        materialized="table",
    )
}}

{# {{
    apply_logic_process_data_p_l(
        "excel", ref("tech_stg_excel_qdtek_tb_north_4001"), '10','4001'
    )
}} #}
with 
    raw_tb_non_accum as (
        select 
            row_number() over (order by (select 1)) ::text as number_id,
            '4001'::text as legal,
            period::text,
            '10'::text as curtype,
            _racct ::text as racct,
            nvl(end_debit_balance,0) - nvl(end_credit_balance,0) as balance,
            ''::text as profit_center
        from {{ ref("tech_stg_excel_qdtek_tb_north_4001") }} a
        where code_pl != 0
    )
select * from raw_tb_non_accum
