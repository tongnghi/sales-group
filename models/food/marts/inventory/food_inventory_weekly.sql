with filter_month as (
    select 
        date_time as at_date,
        year_month,
        material,
        company_code,
        storage_location,
        plant,

        sum(closing_stock) as closing_stock,
        sum(closing_value) as original_value

    from {{ ref('food_inventory_w') }}
    where date_time >= to_char(getdate() - interval '1 month', 'YYYYMMDD')
    group by date_time, year_month, material, plant, company_code, storage_location

)

select 
    filter_month.*,
    pricing.base_price * round(original_value,2) as closing_value

from filter_month
left join {{ ref('food_base_price') }} pricing using (material, plant, year_month)