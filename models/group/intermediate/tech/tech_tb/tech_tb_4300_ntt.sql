{{
    config(
        materialized="table",
    )
}}

{# 
{{
    apply_logic_process_data_consolidates(
        "excel", ref("tech_stg_excel_ntt_tb_ntt_4300"), '10','ntt'
    )
}} #}

with 
    raw_tb_4300_accum as (
        select * , 
        'ntt'::text as legal,
        '10'::text as curtype,
        ''::text as profit_center
        from {{ ref("tech_stg_excel_ntt_tb_ntt_4300") }} a
        where code_pl != 0 and left(_racct,1) in ('5', '6', '7', '8')
    ),
    tb_4300_period as (
        select  period as new_period
        from raw_tb_4300_accum
        group by period
    ),
    tb_4300_raw_full_period as (
        select  _racct,
                new_period
        from tb_4300_period
        cross JOIN raw_tb_4300_accum
    
    ),
    tb_final_racct_period as (
        select
            distinct 
                _racct,
                new_period
        from tb_4300_raw_full_period
    ),
    final_data as (
        select 
            t._racct, t.new_period, r.debit, r.credit,r.legal, r.profit_center,
            case   
                when t.new_period = r.period then nvl(r.debit,0) - nvl(r.credit,0)
                else 0
            end as balance
        from tb_final_racct_period t
        left join raw_tb_4300_accum r
        on t._racct = r._racct and t.new_period = r.period
    ),
    tb_acum as (
        select 
            'ntt'::text as legal,
            new_period::text as period,
            '10'::text as curtype,
            _racct::text as racct,
            sum(balance) over (partition by _racct order by new_period rows unbounded preceding) balance, 
            profit_center::text
        from final_data
        order by new_period
    ),

    raw_tb_4300_not_accum as (
        select  
        'ntt'::text as legal,
        period::text,
        '10'::text as curtype,
        _racct::text as racct,
        nvl(end_debit_balance,0) - nvl(end_credit_balance,0) as balance,
        ''::text as profit_center
        from {{ ref("tech_stg_excel_ntt_tb_ntt_4300") }} a
        where code_pl != 0 and left(_racct,1) not in ('5', '6', '7', '8')
    )

    select * 
    from raw_tb_4300_not_accum
    union all 
    select * 
    from tb_acum
    



