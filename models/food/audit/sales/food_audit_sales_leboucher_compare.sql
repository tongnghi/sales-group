{% set max_date %}
  select
    max(posting_date) as max_date
  from marts_sales.leboucher_sales
{% endset %}

{% set old_sales_leboucher %}
  select
    {{ dbt_utils.generate_surrogate_key([
          'posting_date', 'posted_date', 'ref_no', 'customer_code', 'invoice_date', 'item_code'
      ])
    }} as _key,
    posting_date,  -- ngày chứng từ
    posted_date,  -- ngày hoạch toán
    ref_no,  -- số chứng từ
    customer_code,  -- mã khách hàng
    customer_name,  -- tên khách hàng
    invoice_date,  -- ngày hóa đơn
    invoice_no,  -- số hóa đơn
    item_code,  -- mã hàng
    unit_name,
    --
    channel_code,
    channel_name,  -- tên kênh bán hàng
    sales_group_code,
    sales_group_name,  -- tên nhóm bán hàng
    customer_group_code,
    customer_group_name,  -- tên nhóm khách hàng
    customer_group_1_code,
    customer_group_1_name,  -- tên nhóm khách hàng 1
    customer_group_2_code,
    customer_group_2_name,  -- tên nhóm khách hàng 2
    -- METRICS
    quantity,  -- Số lượng bán
    unit_price,
    main_quantity,  -- Số lượng bán theo ĐVC
    credit_amount,  -- chiết khấu
    sales_amount,  -- Doanh số bán
    vat,  -- Thuế GTGT
    total_payment,  -- tổng thanh toán
    main_unit_price,  -- Đơn giá theo ĐVC
    returned_quantity  -- Tổng số lượng trả lại    
  from {{ ref('food_leboucher_sales') }}
  where posting_date <= ({{ max_date }})
{% endset %}

{% set new_sales_leboucher %}
  select
      {{ dbt_utils.generate_surrogate_key([
          'posting_date', 'posted_date', 'ref_no', 'customer_code', 'invoice_date', 'item_code'
      ])
    }} as _key,
    posting_date,  -- ngày chứng từ
    posted_date,  -- ngày hoạch toán
    ref_no,  -- số chứng từ
    customer_code,  -- mã khách hàng
    customer_name,  -- tên khách hàng
    invoice_date,  -- ngày hóa đơn
    invoice_no,  -- số hóa đơn
    item_code,  -- mã hàng
    unit_name,
    --
    channel_code,
    channel_name,  -- tên kênh bán hàng
    sales_group_code,
    sales_group_name,  -- tên nhóm bán hàng
    customer_group_code,
    customer_group_name,  -- tên nhóm khách hàng
    customer_group_1_code,
    customer_group_1_name,  -- tên nhóm khách hàng 1
    customer_group_2_code,
    customer_group_2_name,  -- tên nhóm khách hàng 2
    -- METRICS
    quantity,  -- Số lượng bán
    unit_price,
    main_quantity,  -- Số lượng bán theo ĐVC
    credit_amount,  -- chiết khấu
    sales_amount,  -- Doanh số bán
    vat,  -- Thuế GTGT
    total_payment,  -- tổng thanh toán
    main_unit_price,  -- Đơn giá theo ĐVC
    returned_quantity  -- Tổng số lượng trả lại    
  from marts_sales.leboucher_sales
{% endset %}

{{
    audit_helper.compare_queries(
        a_query=old_sales_leboucher,
        b_query=new_sales_leboucher,
        primary_key="_key",
        summarize=true,
    )
}}
