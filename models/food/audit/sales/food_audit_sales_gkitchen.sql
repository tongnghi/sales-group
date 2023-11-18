{% set old_sales_gkitchen %}
  select
    to_date(posting_date, 'DD.MM.YYYY') as _posting_date,
    sum(net_amount) as sales_amount,
    sum(quantity) as quantity,
    sum(quantity_unit) as bill_quantity_in_kg
  from tests.old_sales_gkitchen
  where _posting_date between '20230510' and '20230520'
  group by _posting_date
{% endset %}

{% set new_sales_gkitchen %}
  select
    posting_date::date as _posting_date,
    sum(bill_net_amount) as sales_amount,
    sum(bill_quantity) as quantity,
    sum(bill_quantity_in_kg) as bill_quantity_in_kg
  from {{ ref('food_gkitchen_sales') }}
  where _posting_date between '20230510' and '20230520'
  group by _posting_date
{% endset %}

{{
    audit_helper.compare_queries(
        a_query=old_sales_gkitchen,
        b_query=new_sales_gkitchen,
        primary_key="_posting_date",
        summarize=true,
    )
}}
