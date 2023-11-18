with
    raw_data as (
        select customer_id, billing_date, actual_amount
        from {{ ref("tech_qdtek_int_sales__actual_sales__joined") }}
    ),

    ultimate as (
        select
            customer_id,
            max(billing_date) as latest_order,
            sum(actual_amount) as sum_amount
        from raw_data
        group by customer_id
    )

select *
from ultimate
