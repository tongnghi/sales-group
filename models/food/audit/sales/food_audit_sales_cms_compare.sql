{%- set target_relation = api.Relation.create(
      database='food', 
      schema='marts_sales', 
      identifier='cms_billing_sales') -%}

{% set max_date %}
  select
    max(invoice_date) as max_date
  from {{ target_relation }}
{% endset %}

{% set old_sales_cms %}

  select
    {{ dbt_utils.generate_surrogate_key([
        'invoice_date',
        'ship_date',
        'order_number',
        'customer_id',
        'product_code',
        'channel_code',
        'customer_group_code',
        'customer_group_1_code',
        'customer_group_2_code',
        'order_type',
        'ship_name',
        'sub',
        'status',
        'promotion',
        'promotion_type',
        'site_id'
        ])
    }} as _key,  
    {{ dbt_utils.star(from=ref('food_cms_billing_sales'), except=["_key","product_name","origin_customer_group_name","origin_customer_group_1_name","origin_customer_group_2_name"]) }}

  from {{ ref('food_cms_billing_sales') }}
  where invoice_date <= ({{ max_date }})

{% endset %}

{% set new_sales_cms %}
  select
    {{ dbt_utils.star(from=target_relation, except=["origin_customer_group_name","origin_customer_group_1_name","origin_customer_group_2_name"])}}

  from {{ target_relation }}
{% endset %}

{{
    audit_helper.compare_queries(
        a_query=old_sales_cms,
        b_query=new_sales_cms,
        primary_key="_key",
        summarize=true,
    )
}}
