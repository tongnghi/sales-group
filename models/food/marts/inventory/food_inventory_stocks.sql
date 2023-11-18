with stk_historical as (

    select
        date_time,
        year_month,
        material,
        plant,
        company_code,
        storage_location,

        sum(pretotstk_inflow) as pretotstk_inflow,
        sum(pistotstk_outflow) as pistotstk_outflow,
        sum(prevs_val_inflow) as prevs_val_inflow,
        sum(pisvs_val_outflow) as pisvs_val_outflow,

        sum(closing_stock) as closing_stock,
        sum(closing_value) as closing_value,

        sum(issues_stock) as issues_stock,
        sum(reciept_stock) as reciept_stock

    from {{ ref('food_inventory_w') }}
    group by date_time, year_month, material, plant, company_code, storage_location

),

agg_month as (

    select
        year_month,
        material,
        plant,
        company_code,
        storage_location,

        sum(pretotstk_inflow) as pretotstk_inflow,
        sum(pistotstk_outflow) as pistotstk_outflow,
        sum(prevs_val_inflow) as prevs_val_inflow,
        sum(pisvs_val_outflow) as pisvs_val_outflow,

        sum(issues_stock) as issues_stock,
        sum(reciept_stock) as reciept_stock

    from stk_historical
    group by year_month, material, plant, company_code, storage_location

),

usaged_ovt as (

    select 
        *,

        avg(issues_stock) over ( partition by material, plant, company_code, storage_location order by date_time rows 60 preceding) as avg_usage_60d,
        avg(reciept_stock) over ( partition by material, plant, company_code, storage_location order by date_time rows 60 preceding) as avg_manu_60d,

        sum(issues_stock) over ( partition by material, plant, company_code, storage_location order by date_time rows 15 preceding) as sum_usage_15d,
        sum(reciept_stock) over ( partition by material, plant, company_code, storage_location order by date_time rows 15 preceding) as sum_manu_15d

    from stk_historical

),

end_of_month as (

    select 
        date_time as at_date,
        year_month,
        material,
        plant,
        company_code,
        storage_location,
        phy_sloc.physical_wh,

        agg_month.pretotstk_inflow,
        agg_month.pistotstk_outflow,
        agg_month.prevs_val_inflow,
        agg_month.pisvs_val_outflow,
        closing_stock,
        closing_value as original_value,
        pricing.base_price * round(closing_stock,2) as closing_value,
        pricing.base_price as unit_price,
        agg_month.issues_stock,
        agg_month.reciept_stock,
        avg_usage_60d,
        avg_manu_60d,
        sum_usage_15d,
        sum_manu_15d,

        md_product.name as material_name,
        md_product.type_name as material_type,
        md_product.net_weight,
        md_product.unit,
        md_product.ph4_code,
        md_product.cat_code,
        md_product.cat_name,
        md_product.ph5_code,
        md_product.subcat1_code,
        md_product.subcat1_name,
        md_product.subcat2_code,
        md_product.subcat2_name,
        md_product.minimum,
        md_product.maximum,
        md_product.optimal,
        -- avg_usage_60d = 0 phân loại high slow moving
        case when round(closing_stock,2) > 0 and avg_usage_60d != 0 then closing_stock/avg_usage_60d 
            when round(closing_stock,2) > 0 and avg_usage_60d = 0 then 999 
            else 0 end as coverage_days,

        coverage_days - nvl(md_product.maximum,60) as overdays,

        case when coverage_days <= md_product.minimum then 'Low' 
            when coverage_days > md_product.maximum then 'High'
            when md_product.minimum < coverage_days and coverage_days <= md_product.maximum then 'Mid'
                else 'Undefined'
            end as coverage_label,
        
        case when (sum_usage_15d + sum_manu_15d) > 0 then 0 else 1 end as slow_moving_15,
        case when overdays > 0 then 1 else 0 end as slow_moving_60,

        case when avg_usage_60d = 0 then closing_stock
            when slow_moving_60 then overdays * avg_usage_60d else 0 end as slow_moving_quantity,

        slow_moving_quantity * pricing.base_price as slow_moving_value,
        {# slow_moving_value/sum(closing_value) as slow_moving_ratio, #}

        row_number() over (partition by year_month, material, plant, company_code, storage_location order by date_time desc) AS end_month

    from usaged_ovt
    left join {{ ref('food_inventory_products') }} md_product on md_product.code = usaged_ovt.material
    left join agg_month using (year_month, material, plant, company_code, storage_location)
    left join {{ ref('food_stg_excel_inventory__mapping_wh') }} phy_sloc using (plant, storage_location)
    left join {{ ref('food_base_price') }} pricing using (material, plant, year_month)

)

select 
    * 
from end_of_month
where end_month = 1