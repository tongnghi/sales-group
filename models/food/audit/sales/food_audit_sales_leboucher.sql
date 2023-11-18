{% set old_sales_leboucher %}
  select
    to_date(posting_date, 'DD.MM.YYYY') as _posting_date,
    sum(revenue) as sales_amount,
    sum(quantity) as quantity
  from food.marts_sales.leboucher_sales
  where _posting_date between '20230401' and '20230429'
  group by _posting_date
{% endset %}

{% set new_sales_leboucher %}
  select
    posting_date::date as _posting_date,
    sum(sales_amount) as sales_amount,
    sum(quantity) as quantity
  from {{ ref('food_leboucher_sales') }}
  where _posting_date between '20230401' and '20230429'
  group by _posting_date
{% endset %}

{{
    audit_helper.compare_queries(
        a_query=old_sales_leboucher,
        b_query=new_sales_leboucher,
        primary_key="_posting_date",
        summarize=true,
    )
}}
