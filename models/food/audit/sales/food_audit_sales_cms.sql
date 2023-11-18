{% set old_sales_cms %}
  select
    to_date(posting_date, 'DD.MM.YYYY') as _posting_date,
    sum(net_amount) as sales_amount,
    sum(quantity) as quantity
  from food.marts_sales.cms_billing_sales
  where _posting_date between '20230401' and '20230430'
  group by _posting_date
{% endset %}

{% set new_sales_cms %}
  select
    ship_date::date as _posting_date,
    sum( bill_net_amount) as sales_amount,
    sum( billed_quantity) as quantity
  from {{ ref('food_cms_billing_sales') }}
  where _posting_date between '20230401' and '20230430'
  group by _posting_date
{% endset %}

{{
    audit_helper.compare_queries(
        a_query=old_sales_cms,
        b_query=new_sales_cms,
        primary_key="_posting_date",
        summarize=true,
    )
}}
