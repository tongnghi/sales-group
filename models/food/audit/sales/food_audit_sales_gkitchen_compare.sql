{% set old_sales_gkitchen %}
  select
    {{ dbt_utils.generate_surrogate_key([
          'posting_date', 'company_code', 'plant_code', 'sales_office_code', 'sales_organization_code',
          'sales_group_code', 'sales_district_code', 'distribution_channel_code', 'product_code', 'product_hierachy_code',
          '"product_hierarchy_01-1_code"', '"product_hierarchy_01-2_code"', '"product_hierarchy_01-3_code"', '"product_hierarchy_01-4_code"',
          '"product_hierarchy_01-5_code"', '"product_hierarchy_01-6_code"', '"product_hierarchy_01-7_code"', '"product_hierarchy_01-8_code"',
          'customer_group_code', 'sold_to', 'customer_group_1_code', 'customer_group_2_code', '"g/l_account_code"', 'division_code'
      ])
    }} as _key,
    *
  from marts_sales.gkitchen_sales_bk_2023_22_06
{% endset %}

{% set new_sales_gkitchen %}
  select
    *
  from marts_sales.gkitchen_sales
{% endset %}

{{
    audit_helper.compare_queries(
        a_query=old_sales_gkitchen,
        b_query=new_sales_gkitchen,
        primary_key="_key",
        summarize=true,
    )
}}
