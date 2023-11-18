with stk as (

    select 
        *,
        row_number() over (partition by year_month, material, plant, company_code, storage_location, batch_id order by date_time desc) AS end_month

    from {{ ref('food_inventory_w') }}

),

closing_by_month as (

    select 
        year_month, 
        material,
        plant,
        company_code,
        storage_location,
        batch_id,
        date_time as at_date,
        closing_stock,
        closing_value

    from stk
    where end_month = 1

),

aggregated as (

    select
        year_month, 
        material,
        plant,
        company_code,
        storage_location,
        batch_id,

        max(date_time) as at_date,
        sum(pretotstk_inflow) as pretotstk_inflow,
        sum(pistotstk_outflow) as pistotstk_outflow,
        sum(prevs_val_inflow) as prevs_val_inflow,
        sum(pisvs_val_outflow) as pisvs_val_outflow,

        sum(issues_stock) as issues_stock,
        sum(reciept_stock) as reciept_stock

    from stk
    group by year_month, material, plant, company_code, storage_location, batch_id

),

final as (

    select 
        closing_by_month.year_month, 
        closing_by_month.material,
        closing_by_month.plant,
        closing_by_month.company_code,
        closing_by_month.storage_location,
        closing_by_month.batch_id,
        closing_by_month.at_date,
        closing_by_month.closing_stock,
        closing_by_month.closing_value as original_value,
        pricing.base_price * round(closing_by_month.closing_stock,2) as closing_value,
        phy_sloc.physical_wh,

        batch_attr.recal_expired_date as expired_date,
        batch_attr.manu_date as manu_date,
        batch_attr.recal_expired_date - closing_by_month.at_date::date as remain_used_days,
        case
            when remain_used_days is null then 'Undefined'
            when remain_used_days <= 0 then 'E'
            when remain_used_days < batch_attr.warning_days then 'W'
        else 'S' end aging_label

    from closing_by_month
    left join {{ ref('food_inventory_batch_attributes') }} batch_attr on batch_attr.charg = closing_by_month.batch_id 
        and batch_attr.material = closing_by_month.material
    left join {{ ref('food_stg_excel_inventory__mapping_wh') }} phy_sloc using (plant, storage_location)
    left join {{ ref('food_base_price') }} pricing on pricing.material = closing_by_month.material and pricing.plant = closing_by_month.plant
        and pricing.year_month = closing_by_month.year_month
    
)

select 
    *
from final