select 
    *
from {{ source("food_excel_sales", "master_data_customer_lbc") }}