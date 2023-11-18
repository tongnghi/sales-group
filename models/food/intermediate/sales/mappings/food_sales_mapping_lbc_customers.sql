select
    trim("mã khách hàng") as customer_code,
    trim(channel) as channel,
    trim("customer group") as customer_group,
    trim("customer group 1") as customer_group_1,
    trim("customer group 2") as customer_group_2,
    trim("sale group") as sales_group,
    trim("ship_to_name") as ship_to_name,
    trim("ship_to") as ship_to

from {{ ref("base_excel_sales__master_data_customer_lbc") }}
where customer_code is not null
