{{
    config(
        materialized="table",
    )
}}

{# select * from {{ref("tech_stg_excel_qdtek_tb_north_4001")}}
where code_bs != 0 #}
with 
    raw_tb_4001_accum as (
        select * , 
            '4001'::text as legal,
            '10'::text as curtype,
            ''::text as profit_center
        from {{ ref("tech_stg_excel_qdtek_tb_north_4001") }} a
        where code_pl != 0
    ),
    tb_4001_period as (
        select  period as new_period
        from raw_tb_4001_accum
        group by period
    ),
    tb_4001_raw_full_period as (
        select  _racct,
                new_period
        from tb_4001_period
        cross JOIN raw_tb_4001_accum
    
    ),
    tb_final_racct_period as (
        select
            distinct 
                _racct,
                new_period
        from tb_4001_raw_full_period
    ),
    final_data as (
        select 
            t._racct, t.new_period, r.debit, r.credit,r.legal, r.profit_center,
            case   
                when t.new_period = r.period then nvl(r.end_debit_balance,0) - nvl(r.end_credit_balance,0)
                else 0
            end as balance
        from tb_final_racct_period t
        left join raw_tb_4001_accum r
        on t._racct = r._racct and t.new_period = r.period
    ),
    tb_acum as (
        select 
            row_number() over (order by (select 1)) ::text as number_id,
            '4001'::text as legal,
            new_period::text as period,
            '10'::text as curtype,
            _racct::text as racct,
            sum(balance) over (partition by _racct order by new_period rows unbounded preceding) balance, 
            ''::text as profit_center
        from final_data
        order by new_period
    ),

    raw_tb_4001_not_accum as (
        select         
            row_number() over (order by (select 1)) ::text as number_id,
            '4001'::text as legal,
            period::text,
            '10'::text as curtype,
            _racct::text as racct,
            nvl(end_debit_balance,0) - nvl(end_credit_balance,0) as balance,
            ''::text as profit_center
        from {{ ref("tech_stg_excel_qdtek_tb_north_4001") }} a
        where code_bs != 0
    )
    
    select * 
    from raw_tb_4001_not_accum 
    union all 
    select * 
    from tb_acum
  
    

    



