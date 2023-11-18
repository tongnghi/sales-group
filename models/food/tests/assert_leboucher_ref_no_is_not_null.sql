select * from {{ ref("food_leboucher_sales") }} where document_number is null limit 1
