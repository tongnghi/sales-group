with
    raw_data as (
        select * from {{ ref("tech_qdtek_int_sales__actual_sales__joined") }}
    ),

    ultimate as (
        select
            billing_number,
            sum(actual_amount) as net_amount,
            (
                case
                    when sum(actual_amount) >= 200000000
                    then 'Type 1 (<= 200 Mil)'
                    when sum(actual_amount) >= 100000000
                    then 'Type 2 (>= 100 Mil)'
                    when sum(actual_amount) >= 50000000
                    then 'Type 3 (>= 50 Mil)'
                    when sum(actual_amount) >= 10000000
                    then 'Type 4 (>= 10 Mil)'
                    else 'Type 5 (< 10 Mil)'
                end
            )::text as billing_type_name
        from raw_data
        group by billing_number
    )

select *
from ultimate
