with renamed as (

    select
        -- DIMENSIONS
        invoicedate as invoice_date,
        shipdate as ship_date,
        ordernbr as order_number,
        custid as customer_id,
        upper(invtid) as product_code,
        trandesc as product_name,
        lpad(upper(invtid), 18, '0') as product_code_padded,
        channel as origin_channel_name,
        customergroup as origin_customer_group_name,
        customergroup1 as origin_customer_group_1_name,
        customergroup2 as origin_customer_group_2_name,
        ordertype as order_type,
        shipname as ship_name,
        shiptoid as ship_to,
        transportfee,
        sub as sub,
        status as status,
        program as promotion,
        promotiontype as promotion_type,
        siteid as site_id,
        paymnetid as payment_method,

        -- METRICS
        aftertaxamt as aftertax_amount,
        curytaxamt as tax_amount,
        disamt as discount_amount,
        afdamt as bill_net_amount,  -- bill - net amount
        actqty as bill_quantity

    from {{ source("food_sol_gnf1001apprepl", "vs_safoorderl_salesadmin") }}

)

select * from renamed
