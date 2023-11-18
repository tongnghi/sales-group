-- Note: 2 file cần update từ user
select
    {{ dbt_utils.generate_surrogate_key(["general_ledger_id"]) }} as _key,
    -- DIMENSIONS
    posting_date,  -- ngày chứng từ
    posted_date,  -- ngày hoạch toán
    ref_no as document_number,  -- số chứng từ
    customer_code as sold_to,  -- mã khách hàng
    customer_name as sold_to_name,  -- tên khách hàng
    invoice_date,  -- ngày hóa đơn
    invoice_no,  -- số hóa đơn
    item_code as product_code,  -- mã hàng
    item_name as product_name,
    unit_name,
    order_id,
    so_number as sale_order_number,
    accountobjectaddress,
    shippingaddress,
    --
    channel_code as distribution_channel_code,
    origin_channel_name as origin_distribution_channel_name,
    distchan.name as distribution_channel_name,  -- tên kênh bán hàng
    sales_group_code,
    origin_sales_group_name,
    sgrp.name as sales_group_name,  -- tên nhóm bán hàng
    customer_group_code,
    origin_customer_group_name,
    cusgrp.name as customer_group_name,  -- tên nhóm khách hàng
    customer_group_1_code,
    origin_customer_group_1_name,
    cusgrp1.name as customer_group_1_name,  -- tên nhóm khách hàng 1
    customer_group_2_code,
    origin_customer_group_2_name,
    cusgrp2.name as customer_group_2_name,  -- tên nhóm khách hàng 2
    ship_to_name,
    ship_to,
    -- METRICS
    quantity as bill_quantity,  -- Số lượng bán
    unit_price,
    quantity * cv_rate.conversion_rate as bill_quantity_in_kg,  -- Số lượng bán theo ĐVC
    credit_amount,  -- chiết khấu
    sales_amount as bill_net_amount,  -- Doanh số bán
    vat,  -- Thuế GTGT
    total_payment,  -- tổng thanh toán
    main_unit_price,  -- Đơn giá theo ĐVC
    returned_quantity,  -- Tổng số lượng trả lại
    returned_quantity * cv_rate.conversion_rate as returned_quantity_in_kg

    
from {{ ref("food_int_misa_lbc2022_generalledger__filtered_to_sales") }} lbc

left join
    {{ ref("food_int_sales_groups__unioned") }} sgrp on lbc.sales_group_code = sgrp.code

left join
    {{ ref("food_int_channels__unioned") }} distchan on lbc.channel_code = distchan.code

left join
    {{ ref("food_int_customer_groups__unioned") }} cusgrp
    on lbc.customer_group_code = cusgrp.code

left join
    {{ ref("food_int_customer_groups_1__unioned") }} cusgrp1
    on lbc.customer_group_1_code = cusgrp1.code

left join
    {{ ref("food_int_customer_groups_2__unioned") }} cusgrp2
    on lbc.customer_group_2_code = cusgrp2.code

left join 
    {{ ref("food_stg_excel_sales__md_products_conversion_rate_lbc") }} cv_rate
    on lbc.item_code = cv_rate.code and lbc.unit_name = cv_rate.unit