{{
    config(
        materialized="table",
    )
}}
{# 
with raw_data as (
    {{
        apply_logic_process_data_consolidates(
                "excel", ref("feed_stg_excel_donavet_tb_donavet_1100"), '10','donavet'
        )
    }}
),
final_data as (
    select number_id, period , legal,curtype, balance, profit_center, '00' || racct as racct
    from raw_data
) #}

with 
    raw_tb_1005_accum as (
        select *,
        '10'::text as curtype,
        '00'::text ||_racct::text as racct,
        company_code as legal, 
         accumulated_balance as balance
        from {{ ref("feed_stg_excel_tayninh_tb_tayninh_1005") }} a
    )
    select   
        legal::text,
        curtype::text,
        period::text,
        racct::text,
        balance::decimal(20,2),
        profit_center::text 
    from raw_tb_1005_accum
    
