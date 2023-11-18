with
    raw_data as (
        select distinct customer_id, customer_name, customer_group_name
        from {{ ref("tech_qdtek_int_sales__actual_sales__joined") }}
    ),

    ultimate as (
        select
            customer_id,
            split_part(
                substring(replace(customer_group_name, '    ', '_'), 6), '_', 1
            ) as customer_group_id,
            split_part(
                substring(replace(customer_group_name, '    ', '_'), 6), '_', 2
            ) as old_customer_group_name,
            replace(
                customer_name,
                'Vốn kinh doanh ở các đơn vị trực thuộc',
                'INTERAL_Nội bộ'
            ) as customer_name,
            case
                when
                    split_part(
                        substring(replace(customer_group_name, '    ', '_'), 6), '_', 2
                    )
                    = 'End-user'
                then 'Key Account'
                else
                    split_part(
                        substring(replace(customer_group_name, '    ', '_'), 6), '_', 2
                    )
            end as new_customer_group_name
        from raw_data
    )

select *
from ultimate
