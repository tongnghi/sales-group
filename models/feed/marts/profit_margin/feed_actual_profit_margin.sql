with ke24_aggregated as (

    select 
        -- day 0 <= 7 W1, 7 and <= 15 W2 , 16 -> <= 23 W3, 24 >= W3
        case when date_part(day, posting_date) <= 7 then 'W1'
            when date_part(day, posting_date) <= 15 then 'W2'
            when date_part(day, posting_date) <= 23 then 'W3'
            when date_part(day, posting_date) >= 24 then 'W4' end as week,

        dis_channel as distribution_channel,
        sales_group as bu_sales,
        plant,
        sales_office as location_id,
        material_group,
        division,
        material,
        sum(billed_qty) as billed_qty_,
        sum(factory_price) as factory_price_,
        sum(delivery_fee) as delivery_fee_

    from {{ ref("feed_stg_sap_ecc__ke24") }}
    where perio = '00082023' and material_group like 'C1%%' and sched_line_cat in ('Z1','Z2','Z4','Z5','Z6') 
    group by week, distribution_channel,bu_sales,plant,location_id,material_group,division,material

),

actual as (
-- distinct do khi join voi cac ban load k co cot week
    select  

        ke24_aggregated.week,
        ke24_aggregated.distribution_channel,
        ke24_aggregated.bu_sales,
        ke24_aggregated.plant,
        ke24_aggregated.location_id,
        ke24_aggregated.material_group,
        ke24_aggregated.division,
        ke24_aggregated.material as invt_id,
        md_product.material_description as invt_description,
        (ke24_aggregated.factory_price_ + ke24_aggregated.delivery_fee_)/ke24_aggregated.billed_qty_ as base_price_,
        (ke24_aggregated.factory_price_ + ke24_aggregated.delivery_fee_)/ke24_aggregated.billed_qty_ as invc_price_before_vat_,
        (ke24_aggregated.factory_price_ + ke24_aggregated.delivery_fee_)/ke24_aggregated.billed_qty_ as invc_price_after_vat_,
        nvl(ke24_aggregated.billed_qty_,0) as sales_volume_,
        nvl(formula_price.gia_donvi,0) as formula_price_,
        nvl(formula_price.giabaobi,0) as bag_amount_,
        nvl(actual_cogs.shrink,0) * formula_price_ as total_shrink_amount_,
        nvl(actual_adcom.rev_dkg,0) as revfx_amount_,
        nvl(actual_mnf.mnf_v,0) as mnf_varcost_,
        nvl(actual_adcom.commer_vardichdanh,0) as commer_vardichdanh_ ,
        nvl(actual_adcom.commer_varkdichdanh,0) as commer_varkdichdanh_ ,
        nvl(actual_adcom.com_var,0) as commer_varcost_,
        nvl(actual_adcom.fin_var,0) as finan_cost_,
        nvl(actual_mnf.mnf_f,0) as mnf_fixcost_,
        nvl(actual_adcom.commer_fixdichdanh,0) as commer_fixdichdanh_,
        nvl(actual_adcom.commer_fixkdichdanh,0) as commer_fixkdichdanh_,
        nvl(actual_adcom.com_varfix,0) as commer_fixcost_,
        nvl(actual_adcom.admin_fixed,0) as admin_cost_
        
    from ke24_aggregated
    left join {{ ref("feed_stg_sap_ecc__mm60") }} md_product using (plant, material)
    left join {{ ref("feed_formula_price") }} formula_price on ke24_aggregated.plant = formula_price.plant and ke24_aggregated.material = formula_price.material
    left join {{ ref("feed_stg_excel_margin__actual_cogs") }} actual_cogs
        on ke24_aggregated.material = actual_cogs.product and ke24_aggregated.plant = actual_cogs.plant
    left join {{ ref("feed_stg_excel_margin__actual_ad_com") }} actual_adcom 
        on ke24_aggregated.distribution_channel = actual_adcom.dis_channel
        and ke24_aggregated.bu_sales = actual_adcom.sales_group
        and ke24_aggregated.plant = actual_adcom.plant
        and ke24_aggregated.location_id= actual_adcom.sales_office
        and ke24_aggregated.material_group = actual_adcom.material_group
        and ke24_aggregated.division = actual_adcom.division
    left join {{ ref("feed_stg_excel_margin__actual_mnf") }} actual_mnf 
        on ke24_aggregated.plant = actual_mnf.plant
        and ke24_aggregated.material = actual_mnf.product
        and ke24_aggregated.bu_sales = actual_mnf.sales_group
    where ke24_aggregated.billed_qty_ <> 0 -- loại bỏ những trường hợp #0
    
),

actual_final as (

    select
        actual.week,
        actual.distribution_channel, 
        actual.bu_sales,
        actual.plant,
        actual.location_id,
        actual.material_group,
        actual.division,
        actual.invt_id,
        actual.invt_description,
        avg(actual.base_price_) as base_price,
        avg(actual.invc_price_before_vat_) as invc_price_before_vat,
        avg(actual.invc_price_after_vat_) as invc_price_after_vat,
        avg(actual.formula_price_) as formula_price,
        avg(actual.bag_amount_) as bag_amount,
        avg(actual.total_shrink_amount_) as total_shrink_amount,
        avg(actual.revfx_amount_) as revfx_amount,
        avg(actual.commer_vardichdanh_) as commer_vardichdanh,
        avg(actual.commer_varkdichdanh_) as commer_varkdichdanh,
        avg(actual.finan_cost_) as finan_cost,
        avg(actual.commer_varcost_) as commer_varcost,
        avg(actual.mnf_varcost_) as mnf_varcost,
        avg(actual.mnf_fixcost_) as mnf_fixcost,
        avg(actual.commer_fixdichdanh_) as commer_fixdichdanh,
        avg(actual.commer_fixkdichdanh_) as commer_fixkdichdanh,
        avg(actual.commer_fixcost_) as commer_fixcost,
        avg(actual.admin_cost_) as admin_cost,
        avg(actual.sales_volume_) as sales_volume,
        formula_price + bag_amount + total_shrink_amount + revfx_amount as cogs,

        case when actual.distribution_channel = '10' then  avg(discount.discount_dkg)
            when actual.distribution_channel = '30' then 0
            else invc_price_after_vat - cogs - commer_varcost - mnf_varcost - mnf_fixcost
        end as total_discount_amt,

        invc_price_after_vat - total_discount_amt as net_sales,

        net_sales - cogs as gross_margin,
        gross_margin/net_sales as pct_gm_ns,

        gross_margin - (mnf_varcost + commer_varcost + finan_cost) as gm_aft_vc,
        gm_aft_vc/net_sales as pct_gm_aft_vc_ns,

        gm_aft_vc - (mnf_fixcost + commer_fixcost + admin_cost) as nm_perunit,
        nm_perunit / net_sales as pct_nm_ns
        
        

    from  actual 
    left join {{ ref("feed_stg_excel_margin__actual_discount") }} discount
        on actual.week = discount.week
        and actual.distribution_channel = discount.dis_channel
        and actual.bu_sales = discount.sales_group
        and actual.location_id = discount.sales_office
        and actual.invt_id = discount.product
    group by actual.week,
                actual.distribution_channel, 
                actual.bu_sales,
                actual.plant,
                actual.location_id,
                actual.material_group,
                actual.division,
                actual.invt_id,
                actual.invt_description 

),

actual_finale as (

    select 
        week,
        distribution_channel, 
        bu_sales,
        plant,
        location_id,
        material_group,
        division,
        invt_id,
        invt_description,
        round(base_price,2)::decimal(30,2) as base_price_,
        round(invc_price_before_vat,2)::decimal(30,2) as invc_price_before_vat_,
        round(invc_price_after_vat,2)::decimal(30,2) as invc_price_after_vat_,
        round(total_discount_amt,2)::decimal(30,2) as total_discount_amt_,
        round(net_sales,2)::decimal(30,2) as net_sales_,
        round(formula_price,2)::decimal(30,2) as formula_price_,
        round(bag_amount,2)::decimal(30,2) as bag_amount_,
        round(total_shrink_amount,2)::decimal(30,2) as total_shrink_amount_,
        round(revfx_amount,2)::decimal(30,2) as revfx_amount_,
        round(cogs,2)::decimal(30,2) as cogs_,
        round(gross_margin,2)::decimal(30,2) as gross_margin_,
        round(pct_gm_ns,2)::decimal(30,2) as pct_gm_ns_,
        round(mnf_varcost,2)::decimal(30,2) as mnf_varcost_,
        round(commer_vardichdanh,2)::decimal(30,2) as commer_vardichdanh_,
        round(commer_varkdichdanh,2)::decimal(30,2) as commer_varkdichdanh_,
        round(commer_varcost,2)::decimal(30,2) as commer_varcost_,
        round(finan_cost,2)::decimal(30,2) as finan_cost_,
        round(gm_aft_vc,2)::decimal(30,2) as gm_aft_vc_,
        round(pct_gm_aft_vc_ns,2)::decimal(30,2) as pct_gm_aft_vc_ns_,
        round(mnf_fixcost,2)::decimal(30,2) as mnf_fixcost_,
        round(commer_fixdichdanh,2)::decimal(30,2) as commer_fixdichdanh_,
        round(commer_fixkdichdanh,2)::decimal(30,2) as commer_fixkdichdanh_,
        round(commer_fixcost,2)::decimal(30,2) as commer_fixcost_,
        round(admin_cost,2)::decimal(30,2) as admin_cost_,
        round(nm_perunit,2)::decimal(30,2) as nm_perunit_,
        round(pct_nm_ns,2)::decimal(30,2) as pct_nm_ns_,
        round(sales_volume,2)::decimal(30,2) as sales_volume_,
        round(sales_volume_ * cogs_,2)::decimal(30,2) as total_cogs_,
        round(sales_volume_ * net_sales_,2)::decimal(30,2)  as total_netsales_,
        round(sales_volume_ * gross_margin_,2)::decimal(30,2)  as total_gm_,
        round(mnf_varcost_ + commer_varcost_ + finan_cost_,2)::decimal(30,2)  as total_varcost_,
        round(total_gm_ - total_varcost_,2)::decimal(30,2)  as total_gm_aft_var_,
        round(mnf_fixcost_ + commer_fixcost_ + admin_cost_,2)::decimal(30,2)  as total_fixcost_,
        round(total_gm_aft_var_ - total_fixcost_,2)::decimal(30,2)  as total_netmargin_,
        round(finan_cost_ * sales_volume_,2)::decimal(30,2)  as financial_cost_
        
    from actual_final 
    order by week asc
)

select * from actual_finale
