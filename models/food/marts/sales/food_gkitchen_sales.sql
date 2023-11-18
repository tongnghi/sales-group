with joined_dims as (

    select
        sales.posting_date,
        sales.period_year,
        sales.company_code,
        sales.plant_code,
        plant.name as plant_name,
        sales.sales_office_code,
        soff.name as sales_office_name,
        sales.sales_organization_code,
        sales.sales_group_code,
        sgrp.name as sales_group_name,
        sales.sales_district_code,
        sdist.name as sales_district_name,
        sales.distribution_channel_code,
        distchan.name as distribution_channel_name,

        ltrim(sales.product_code,'0') as product_code,

        product.name as product_name,
        sales.product_hierachy_code,
        sales.customer_group_code,
        cusgrp.name as customer_group_name,
        
        ltrim(sales.sold_to,'0') as sold_to,

        cus.name as sold_to_name,
        sales.customer_group_1_code,
        cusgrp1.name as customer_group_1_name,
        sales.customer_group_2_code,
        cusgrp2.name as customer_group_2_name,
        sales.gl_account_code,
        sales.division_code,
        sales.billing_type,
        md_billing.name as billing_type_name,
        ltrim(sales.bill_to,'0') as bill_to,
        ltrim(sales.ship_to,'0') as ship_to,
        ship_cus.name as ship_to_name,
        sales.catagory_item as item_category,
        sales.pempz11 as sale_salesman,
        sales.pempz12 as sale_executive,
        sales.pempz13 as sale_supervisor,
        sales.pempz14 as sale_asm,
        sales.pempz15 as sale_rsm,
        sales.document_number,
        sales.psodocno as sale_order_number,
        sales.reason_code,
        rs_text.name as reason,
        sales.bill_quantity,
        sales.unit,
        -- TODO: review should process net_weight type before or cast after apply formula
        (sales.bill_quantity * net_weight)::decimal(15,3) as bill_quantity_in_kg,
        sales.bill_net_amount

    from {{ ref("food_int_fi_ofi_acdoca_10__filtered_to_sales") }} as sales

    left join
        {{ ref("food_stg_sap_s4__md_0plant_text") }} plant on sales.plant_code = plant.code

    left join
        {{ ref("food_stg_sap_s4__md_0sales_off_text") }} soff
        on sales.sales_office_code = soff.code

    left join
        {{ ref("food_int_sales_groups__unioned") }} sgrp 
        on sales.sales_group_code = sgrp.code

    left join
        {{ ref("food_stg_sap_s4__md_0sales_dist_text") }} sdist
        on sales.sales_district_code = sdist.code

    left join
        {{ ref("food_int_channels__unioned") }} distchan
        on sales.distribution_channel_code = distchan.code

    left join
        {{ ref("food_stg_sap_s4__md_0material_text") }} product
        on sales.product_code = product.code

    left join
        {{ ref("food_int_customer_groups__unioned") }} cusgrp
        on sales.customer_group_code = cusgrp.code

    left join {{ ref("food_stg_sap_s4__md_0customer_text") }} cus 
        on sales.sold_to = cus.code

    left join {{ ref("food_stg_sap_s4__md_0customer_text") }} ship_cus 
        on sales.ship_to = ship_cus.code

    left join
        {{ ref("food_int_customer_groups_1__unioned") }} cusgrp1
        on sales.customer_group_1_code = cusgrp1.code

    left join
        {{ ref("food_int_customer_groups_2__unioned") }} cusgrp2
        on sales.customer_group_2_code = cusgrp2.code

    left join 
        {{ ref("food_stg_sap_s4__md_0ord_reason_text") }} rs_text
        on sales.reason_code = rs_text.reason_code

    left join
        {{ ref("food_stg_sap_s4__md_0bill_type_text") }} md_billing
        on sales.billing_type = md_billing.code

)

select
    {{ dbt_utils.generate_surrogate_key([
            'posting_date', 'company_code', 'plant_code', 'sales_office_code', 'sales_organization_code',
            'sales_group_code', 'sales_district_code', 'distribution_channel_code', 'product_code', 'product_hierachy_code',
            'customer_group_code', 'sold_to', 'customer_group_1_code', 'customer_group_2_code', '"gl_account_code"', 'division_code'
        ])
    }} as _key,
    *

from joined_dims