-- deprecated
with
    unioned as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("base_excel_sales__master_data_customer_lbc"),
                    ref("food_seed_sales_mapping_lbc_customers_2023_03_27"),
                ],
                include=[
                    "customer group",
                    "mã khách hàng",
                    "tên khách hàng",
                    "customer group 1",
                    "customer group 2",
                    "channel",
                    "sale group",
                    "ship_to",
                    "ship_to_name",
                ],
            )
        }}
    ),

    deduped as (
        {# {{
            dbt_utils.deduplicate(
                relation="unioned",
                partition_by='"mã khách hàng"',
                order_by="ship_to nulls last",
            )
        }} #}
        select 
            *,
            row_number() over (partition by "mã khách hàng" order by ship_to nulls last) as dedup
        from unioned
    )

select *
from deduped
where dedup = 1 
