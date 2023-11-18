select
    channelcode::varchar as channel_code,
    channel::varchar as channel,
    month::date,
    nvl(budget, 0)::double precision as budget,
    nvl(kpi, 0)::double precision as kpi,
    nvl(forecast, 0)::double precision as forecast
from {{ source("food_excel_sales", "sales_target_by_channels") }}
