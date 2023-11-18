select * from {{ ref("food_audit_sales_cms") }} where percent_of_total < 100
