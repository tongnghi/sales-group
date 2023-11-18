select
    "ChannelCode"::varchar as channel_code,
    "Channel"::varchar as channel,
    "Group category" as group_category,
    "Storage"::varchar as storage,
    "Category"::varchar as category,
    "Class"::varchar as class,
    "Product group"::varchar as product_group,
    "Subcat"::varchar as subcategory,
    "Month"::date as month,
    nvl("Budget", 0)::double precision as budget,
    nvl("KPI", 0)::double precision as kpi,
    nvl("Forecast", 0)::double precision as forecast
from {{ source("food_excel_sales", "sales_target_by_channels_products") }}
