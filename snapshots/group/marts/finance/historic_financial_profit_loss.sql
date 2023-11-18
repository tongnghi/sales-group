{% snapshot historic_financial_profit_loss_snapshot %}

{{
    config(
        target_database='group',
        target_schema='snapshots',
        unique_key='period',
        strategy='check',
        check_cols=[
            'balance'
        ],
    )
}}

select
    period,
    round(sum(balance)) as balance
from {{ ref('financial_profit_loss') }}
where period < to_char(current_timestamp, 'YYYY0MM')
group by period

{% endsnapshot %}