with odbc_channels as (
    
    select * from {{ ref("food_stg_excel_sales__md_channels") }}
    where source = 'ODBC'

),

odbc_customer_group as (

    select * from {{ ref("food_stg_excel_sales__md_customer_groups") }}
    where source = 'ODBC'

),

material as (

    select * 
    from {{ ref('food_int_products__unioned') }}

),

voucher_agg as (

    select 
        order_number,
        sum(voucher_amount) as voucher_amount
    from {{ ref('food_stg_sol__gnf1001apprepl_xt_safoweb_v') }}
    group by order_number

),

mapped as (
    
    select 
        sales.*, 
        odbc_channels.to_code as channel_code,
        odbc_customer_group.to_customer_group_code as customer_group_code,
        odbc_customer_group.to_customer_group_1_code as customer_group_1_code,
        odbc_customer_group.to_customer_group_2_code as customer_group_2_code,
        -- calculated_weight
        (sales.bill_quantity * material.net_weight) as bill_quantity_in_kg,
        sum(bill_net_amount) over (partition by order_number) as total_net_amount,
        nvl(voucher_agg.voucher_amount,0) as total_voucher_amount

    from {{ ref("food_stg_sol__gnf1001apprepl_vsdwh_salesvolumed") }} sales
    left join odbc_channels on sales.origin_channel_name = odbc_channels.from_code
    left join odbc_customer_group
        on sales.origin_customer_group_name = odbc_customer_group.from_customer_group_name
        and sales.origin_customer_group_1_name = odbc_customer_group.from_customer_group_1_name
        and sales.origin_customer_group_2_name = odbc_customer_group.from_customer_group_2_name
    left join material on sales.product_code = material.code
    left join voucher_agg using (order_number)

),

grouped as (

    select
        invoice_date,
        ship_date,
        order_number,
        customer_id,
        product_code,
        product_name,
        channel_code,
        origin_channel_name,
        customer_group_code,
        origin_customer_group_name,
        customer_group_1_code,
        origin_customer_group_1_name,
        customer_group_2_code,
        origin_customer_group_2_name,
        order_type,
        ship_name,
        ship_to,
        sub,
        status,
        promotion,
        promotion_type,
        site_id,
        payment_method,
        sum(total_net_amount) as total_net_amount,
        sum(total_voucher_amount) as total_voucher_amount,
        sum(bill_net_amount) as bill_net_amount,
        sum(bill_quantity) as bill_quantity,
        sum(bill_quantity_in_kg) as bill_quantity_in_kg,
        sum(tax_amount) as tax_amount,
        sum(aftertax_amount) as aftertax_amount,
        sum(discount_amount) as discount_amount,

        sum(
            case when total_net_amount != 0 then total_voucher_amount*bill_net_amount/total_net_amount
                else total_net_amount end
        ) as voucher_amount,
        
        sum(transportfee) as transportfee,
        sum(
            case when total_net_amount != 0 then transportfee*bill_net_amount/total_net_amount
                else transportfee end
        ) as transportfee_line
 
    from mapped {{ dbt_utils.group_by(n=23) }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'invoice_date',
            'ship_date',
            'order_number',
            'customer_id',
            'product_code',
            'channel_code',
            'customer_group_code',
            'customer_group_1_code',
            'customer_group_2_code',
            'order_type',
            'ship_name',
            'sub',
            'status',
            'promotion',
            'promotion_type',
            'site_id',
            'payment_method'
            ])
        }} as _key,
        *
    from grouped

)

select * from final
