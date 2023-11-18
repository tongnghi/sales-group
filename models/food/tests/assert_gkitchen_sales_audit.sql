select * from {{ ref("food_audit_sales_gkitchen") }} where percent_of_total < 100
