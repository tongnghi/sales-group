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
    raw_tb_1100_accum as (
        select * , 
        'donavet'::text as legal,
        '10'::text as curtype,
        ''::text as profit_center
        from {{ ref("feed_stg_excel_donavet_tb_donavet_1100") }} a
        where chi_tiet = 1 and left(_racct,1) in ('5', '6', '7', '8')
    ),
    tb_1100_period as (
        select  period as new_period
        from raw_tb_1100_accum
        group by period
    ),
    tb_1100_raw_full_period as (
        select  _racct,
                new_period
        from tb_1100_period
        cross JOIN raw_tb_1100_accum
    
    ),
    tb_final_racct_period as (
        select
            distinct 
                _racct,
                new_period
        from tb_1100_raw_full_period
    ),
    final_data as (
        select 
            t._racct, t.new_period, r.debit, r.credit, r.chi_tiet, r.legal, r.profit_center,
            case   
                when t.new_period = r.period then nvl(r.debit,0) - nvl(r.credit,0)
                else 0
            end as balance
        from tb_final_racct_period t
        left join raw_tb_1100_accum r
        on t._racct = r._racct and t.new_period = r.period
    ),
    tb_acum as (
        select 
            'donavet'::text as legal,
            new_period::text as period,
            '10'::text as curtype,
            '00' || _racct::text as racct,
            sum(balance) over (partition by _racct order by new_period rows unbounded preceding) balance, 
            profit_center::text
        from final_data
        order by new_period
    ),

    raw_tb_1100_not_accum as (
        select  
        'donavet'::text as legal,
        period::text,
        '10'::text as curtype,
        '00' || _racct::text as racct,
        nvl(end_debit_balance,0) - nvl(end_credit_balance,0) as balance,
        ''::text as profit_center
        from {{ ref("feed_stg_excel_donavet_tb_donavet_1100") }} a
        where chi_tiet = 1 and left(_racct,1) not in ('5', '6', '7', '8')
    )
    select 
        legal::text,
        curtype::text,
        period::text,
        racct::text,
        balance::decimal(20,2),
        profit_center::text
    from raw_tb_1100_not_accum
    union all  
    select   
        legal::text,
        curtype::text,
        period::text,
        racct::text,
        balance::decimal(20,2),
        profit_center::text 
    from tb_acum
    
