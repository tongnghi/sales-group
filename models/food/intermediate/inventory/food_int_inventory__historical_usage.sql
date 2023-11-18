with usage_day as (

    select
        budat,
        matnr,
        werks ,
        sum(pistotstk_outflow) as usage

    from {{ ref('food_int_inventory__openning') }}
    where budat >= to_char(current_date - interval '60 day', 'YYYYMMDD') and type = 'selling'
    group by budat, matnr, werks

),

final as (

    select
        matnr,
        werks,
        avg(usage) as avg_usage_60d
        
    from usage_day
    group by matnr, werks

)

select * from final where avg_usage_60d > 0