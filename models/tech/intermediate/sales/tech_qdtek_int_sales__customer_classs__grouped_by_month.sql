with
    raw_data as (
        select * from {{ ref("tech_qdtek_int_sales__actual_sales__joined") }}
    ),

    ultimate as (
        select
            (left(billing_date, 4)::int) as year,
            (substring(billing_date, 6, 2)::int) as month,

            case
                when substring(billing_date, 6, 2)::int > 12
                then 0
                else substring(billing_date, 6, 2)::int
            end as new_month,

            customer_id,
            old_team_code,
            old_region_code,
            old_industry_code,
            salesman_id,
            sum(actual_amount) as net_amount,

            case
                when sum(actual_amount) >= 3000000000
                then 1
                when sum(actual_amount) >= 1000000000
                then 2
                when sum(actual_amount) >= 500000000
                then 3
                when sum(actual_amount) > 0
                then 4
                else 5
            end as customer_class

        from raw_data
        group by
            left(billing_date, 4),
            substring(billing_date, 6, 2),
            customer_id,
            old_team_code,
            old_region_code,
            old_industry_code,
            salesman_id
    )

select *
from ultimate
