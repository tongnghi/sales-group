with sales as (

    select *
    from {{ ref("food_stg_misa__lbc_generalledger") }}
    where 
        (
            substring(correspondingaccountnumber, 1, 3) = '511' 
            or (
                substring(refno, 1, 2) = 'GG' and substring(correspondingaccountnumber, 1, 3) = '335'
            )
            or (
                substring(correspondingaccountnumber, 1, 3) = '521' and creditamount != 0
            )
        )
        and (
            -- filter tài khoản có
            correspondingaccountnumber not between '33500000' and '33599999'
            and correspondingaccountnumber not between '91100000' and '91199999'
        )
        and refno not in (
                'PK2301-0045',
                'PK2301-0048',
                'PK2301-0063',
                'PK2302-0026',
                'PK2302-0065'
        )

),

customer_code_to_main_dimensions as (

    select
        customers.customer_code,
        channels.to_code as _channel_code,
        customers.channel as origin_channel_name,
        customer_groups.to_customer_group_code as _customer_group_code,
        customers.customer_group as origin_customer_group_name,
        customer_groups.to_customer_group_1_code as _customer_group_1_code,
        customers.customer_group_1 as origin_customer_group_1_name,
        customer_groups.to_customer_group_2_code as _customer_group_2_code,
        customers.customer_group_2 as origin_customer_group_2_name,
        sales_groups.to_sales_group_code as _sales_group_code,
        customers.sales_group as origin_sales_group_name,
        customers.ship_to_name as ship_to_name,
        customers.ship_to as ship_to

    from {{ ref("food_sales_mapping_lbc_customers") }} customers
    left join {{ ref("food_stg_excel_sales__md_channels") }} channels
        on customers.channel = channels.from_code and channels.source = 'LBC'
    left join {{ ref("food_stg_excel_sales__md_customer_groups") }} customer_groups
        on customers.customer_group = customer_groups.from_customer_group_name
        and customers.customer_group_1 = customer_groups.from_customer_group_1_name
        and customers.customer_group_2 = customer_groups.from_customer_group_2_name
        and customer_groups.source = 'LBC'
    left join {{ ref("food_stg_excel_sales__lbc_sales_groups") }} sales_groups
        on customers.sales_group = sales_groups.from_sales_group_name
        and sales_groups.source = 'LBC'

),

mapped_common_dimensions as (

    select
        sales.*,
        customer_code_to_main_dimensions._channel_code,
        customer_code_to_main_dimensions.origin_channel_name,
        customer_code_to_main_dimensions._customer_group_code,
        customer_code_to_main_dimensions.origin_customer_group_name,
        customer_code_to_main_dimensions._customer_group_1_code,
        customer_code_to_main_dimensions.origin_customer_group_1_name,
        customer_code_to_main_dimensions._customer_group_2_code,
        customer_code_to_main_dimensions.origin_customer_group_2_name,
        customer_code_to_main_dimensions._sales_group_code,
        customer_code_to_main_dimensions.origin_sales_group_name,
        customer_code_to_main_dimensions.ship_to_name,
        customer_code_to_main_dimensions.ship_to

    from sales
    left join customer_code_to_main_dimensions
        on sales.accountobjectcode = customer_code_to_main_dimensions.customer_code
),

from_vat as (

    select * from sales
    where substring(correspondingaccountnumber, 1, 3) in ('511', '521')

),

to_vat as (
    -- why '333', It doesn't exist in sales
    select
        refno,
        refdate,
        invno,
        inventoryitemcode,
        (debitamount - creditamount) as vat

    from sales
    where substring(correspondingaccountnumber, 1, 3) = '333'

),

vat as (

    select
        from_vat.generalledgerid,
        
        case
            when substring(from_vat.correspondingaccountnumber, 1, 3) = '521'
            then (-1) * to_vat.vat
            else to_vat.vat
        end as _vat

    from from_vat
    join to_vat on from_vat.refno = to_vat.refno
        and from_vat.refdate = to_vat.refdate
        and from_vat.invno = to_vat.invno
        and from_vat.inventoryitemcode = to_vat.inventoryitemcode

),

final as (

    select
        -- DIMENSIONS
        mapped_common_dimensions.generalledgerid as general_ledger_id,
        mapped_common_dimensions.refdate as posting_date,  -- ngày chứng từ
        mapped_common_dimensions.posteddate as posted_date,  -- ngày hoạch toán
        mapped_common_dimensions.refno as ref_no,  -- số chứng từ
        mapped_common_dimensions.accountobjectcode as customer_code,  -- mã khách hàng
        mapped_common_dimensions.accountobjectname as customer_name,  -- tên khách hang
        mapped_common_dimensions.invdate as invoice_date,  -- ngày hóa đơn
        mapped_common_dimensions.invno as invoice_no,  -- số hóa đơn
        upper(mapped_common_dimensions.inventoryitemcode) as item_code,  -- mã hàng
        mapped_common_dimensions.inventoryitemname as item_name,
        unit.unitname as unit_name,
        mapped_common_dimensions.orderid as order_id,
        so.refno as so_number,
        so.accountobjectaddress,
        so.shippingaddress,
        --
        mapped_common_dimensions._channel_code as channel_code,
        mapped_common_dimensions.origin_channel_name,
        mapped_common_dimensions._sales_group_code as sales_group_code,
        mapped_common_dimensions.origin_sales_group_name,
        mapped_common_dimensions._customer_group_code as customer_group_code,
        mapped_common_dimensions.origin_customer_group_name,
        mapped_common_dimensions._customer_group_1_code as customer_group_1_code,
        mapped_common_dimensions.origin_customer_group_1_name,
        mapped_common_dimensions._customer_group_2_code as customer_group_2_code,
        mapped_common_dimensions.origin_customer_group_2_name,
        mapped_common_dimensions.ship_to_name,
        mapped_common_dimensions.ship_to,
        -- METRICS
        case
            when substring(mapped_common_dimensions.correspondingaccountnumber, 1, 3) = '521'
            then 0
            else mapped_common_dimensions.quantity
        end as quantity,  -- Số lượng bán
        mapped_common_dimensions.unitprice as unit_price,

        case
            when substring(mapped_common_dimensions.correspondingaccountnumber, 1, 3) = '521'
            then 0
            else mapped_common_dimensions.mainquantity
        end as main_quantity,  -- Số lượng bán theo ĐVC

        case
            when
                left(mapped_common_dimensions.correspondingaccountnumber, 3) = '335'
                and left(mapped_common_dimensions.refno, 2) = 'GG'
            then 0
            else mapped_common_dimensions.creditamount
        end as credit_amount,  -- chiết khấu

        (mapped_common_dimensions.debitamount - credit_amount) as sales_amount,  -- Doanh số bán
        case when vat._vat is not null then vat._vat else 0 end as vat,  -- Thuế GTGT
        ((mapped_common_dimensions.debitamount - credit_amount) + vat) as total_payment,  -- tổng thanh toán
        mapped_common_dimensions.mainunitprice as main_unit_price,  -- Đơn giá theo ĐVC

        case
            when substring(mapped_common_dimensions.correspondingaccountnumber, 1, 3) = '521'
            then mapped_common_dimensions.quantity
            else 0
        end as returned_quantity  -- Tổng số lượng trả lại

    from mapped_common_dimensions
    left join vat on mapped_common_dimensions.generalledgerid = vat.generalledgerid
    left join {{ source("food_misa", "unit") }} unit
        on mapped_common_dimensions.unitid = unit.unitid
    left join {{ source("food_misa", "saorder") }} so
        on mapped_common_dimensions.orderid = so.refid

)

select * from final
