select *
from {{ source("stg_excel_margin", "forecast_manufacturing") }}