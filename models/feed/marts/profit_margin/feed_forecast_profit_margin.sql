with ke25 as (

    select 
        dis_channel, 
        sales_group, 
        plant,
        sales_office,
        division, 
        material,
        
        sum(Billed_Qty) as billed_qty,
        sum(Primaterial_cost) as primaterial_cost,
        sum(packaging_cost) as packaging_cost,
        sum(labor_cost) as labor_cost,
        sum(machinery_cost) as machinery_cost,
        sum(electricity_cost) as electricity_cost,
        sum(steam_cost) as steam_cost,
        sum(equipment_cost) as equipment_cost,
        sum(overhead) as overhead

    from {{ ref("feed_stg_sap_ecc__ke25") }}
    -- just filter version = 'QM1'
    group by dis_channel, sales_group, plant, sales_office, division, material

),

forecast as (

    select 
        ke25.dis_channel as distribution_channel,
        ke25.sales_group as bu_sales,
        ke25.plant,
        ke25.sales_office as location_id,
        -- material_group?
        ke25.division,
        ke25.material as invt_id,
        md_product.material_description as invt_description,
        nvl(fore_baseprice.amount,0) as base_price,
        base_price as invc_price_before_vat,
        invc_price_before_vat as invc_price_after_vat,
        nvl(ke25.primaterial_cost/ke25.billed_qty,0) as formula_price,
        nvl(ke25.packaging_cost/ke25.billed_qty,0) as bag_amount,
        nvl(forecast_cogs.shrink,0)* formula_price as total_shrink_amount,
        0 as revfx_amount,
        (formula_price + bag_amount + total_shrink_amount + revfx_amount) as cogs,
        (ke25.electricity_cost + ke25.steam_cost + ke25.equipment_cost + ke25.overhead) * forecast_overhead_var.tyle_var / ke25.billed_qty  as mnf_varcost,
        0 as commer_vardichdanh,
        0 as commer_varkdichdanh,
        0 as commer_varcost,
        0 as finan_cost,
        (ke25.labor_cost + ke25.machinery_cost + ke25.overhead) * forecast_overhead_var.tyle_fix / ke25.billed_qty  as mnf_fixcost,
        0 as commer_fixdichdanh,
        0 as commer_fixkdichdanh,
        0 as commer_fixcost,
        0 as admin_cost,
        ke25.billed_qty as sales_volume,
        sales_volume*cogs as total_cogs,
        sales_volume * revfx_amount as total_revfx_amount,
        sales_volume* (mnf_varcost + commer_varcost + finan_cost) as total_varcost,
        sales_volume* (mnf_fixcost + commer_fixcost + admin_cost) as total_fixcost,
        sales_volume * finan_cost as financial_cost


    from ke25
    left join {{ ref("feed_stg_sap_ecc__mm60") }} md_product using (plant, material)
    left join {{ ref("feed_stg_excel_margin__forecast_base_price") }} fore_baseprice using (dis_channel,sales_office,material )
    left join {{ ref("feed_stg_excel_margin__forecast_overhead_var") }} forecast_overhead_var using (plant, division)
    left join {{ ref("feed_stg_excel_margin__actual_cogs") }} forecast_cogs on ke25.plant = forecast_cogs.plant
        and ke25.material = forecast_cogs.product

),

forecast_final as (

    select
        forecast.distribution_channel,
        forecast.bu_sales,
        forecast.plant,
        forecast.location_id,
        forecast.division,
        forecast.invt_id,
        forecast.invt_description,
        forecast.base_price,
        forecast.invc_price_before_vat,
        forecast.invc_price_after_vat,

        case when forecast.distribution_channel = '10' then  forecast_discount.discount_dkg
            when forecast.distribution_channel = '30' then 0
            else forecast.invc_price_after_vat - forecast.cogs - forecast.commer_varcost - forecast.mnf_varcost - forecast.mnf_fixcost
        end as total_discount_amt,

        invc_price_after_vat - total_discount_amt as net_sales,
        formula_price,
        bag_amount,
        total_shrink_amount,
        revfx_amount,
        cogs,
        net_sales - cogs as gross_margin,
        gross_margin/net_sales as pct_gm_ns,
        mnf_varcost, 
        commer_vardichdanh,
        commer_varkdichdanh,
        commer_varcost,
        finan_cost,
        gross_margin - (forecast.mnf_varcost + forecast.commer_varcost + forecast.finan_cost) gm_aft_vc,
        gm_aft_vc/net_sales as pct_gm_aft_vc_ns,
        mnf_fixcost,
        commer_fixdichdanh,
        commer_fixkdichdanh,
        commer_fixcost,
        admin_cost,
        gm_aft_vc - (forecast.mnf_fixcost + forecast.commer_fixcost + forecast.admin_cost ) as nm_perunit,
        nm_perunit/net_sales as pct_nm_ns,
        sales_volume,
        total_cogs,
        sales_volume* net_sales as total_netsales,
        sales_volume*gross_margin as total_gm,
        total_gm - total_varcost as total_gm_aft_var,
        total_gm_aft_var - total_fixcost as total_netmargin,
        total_revfx_amount,
        total_varcost,
        total_fixcost,
        financial_cost

    from forecast
    left join {{ ref("feed_stg_excel_margin__forecast_discount") }} forecast_discount
        on forecast.distribution_channel = forecast_discount.dis_channel
        and forecast.bu_sales = forecast_discount.sales_group
        and forecast.location_id = forecast_discount.sales_office
        and forecast.invt_id = forecast_discount.material

)

select * from forecast_final