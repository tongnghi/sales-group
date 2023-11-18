select * from {{ ref("food_audit_sales_leboucher") }} where percent_of_total < 100
