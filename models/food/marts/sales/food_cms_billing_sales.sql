select

    _key,
    invoice_date as posting_date,
    ship_date,
    order_number as sale_order_number,
    customer_id as sold_to,
    cms.product_code,
    cms.product_name,
    cms_prd.product_code as cms_product_code,
    cms_prd.product_name as cms_product_name,
    channel_code as distribution_channel_code,
    origin_channel_name as distribution_channel_name,
    -- tên kênh bán hàng
    distchan.name as channel_name,  
    customer_group_code,
    origin_customer_group_name,
    -- tên nhóm khách hàng
    cusgrp.name as customer_group_name,  
    customer_group_1_code,
    origin_customer_group_1_name,
    -- tên nhóm khách hàng 1
    cusgrp1.name as customer_group_1_name,  
    customer_group_2_code,
    origin_customer_group_2_name,
    -- tên nhóm khách hàng 2
    cusgrp2.name as customer_group_2_name,  
    order_type as billing_type_name,
    ship_name as sold_to_name,
    ship_to,
    transportfee,
    transportfee_line,
    sub,
    status,
    promotion,
    promotion_type,
    site_id,
    payment_method,
    bill_net_amount as bill_net_amount,
    bill_quantity as bill_quantity,
    bill_quantity_in_kg as bill_quantity_in_kg,
    total_net_amount,
    total_voucher_amount,
    tax_amount,
    aftertax_amount,
    discount_amount,
    voucher_amount,
    bill_net_amount + tax_amount - discount_amount - voucher_amount as payment_amount

from {{ ref("food_int_sol_gnf1001apprepl_vsdwh_salesvolumed__grouped_to_sales") }} cms
left join {{ ref("food_int_channels__unioned") }} distchan 
    on cms.channel_code = distchan.code
left join {{ ref("food_int_customer_groups__unioned") }} cusgrp 
    on cms.customer_group_code = cusgrp.code
left join {{ ref("food_int_customer_groups_1__unioned") }} cusgrp1
    on cms.customer_group_1_code = cusgrp1.code
left join {{ ref("food_int_customer_groups_2__unioned") }} cusgrp2
    on cms.customer_group_2_code = cusgrp2.code
left join {{ ref('food_stg_cms__md_product') }} cms_prd on cms_prd.sap_product_code = cms.product_code