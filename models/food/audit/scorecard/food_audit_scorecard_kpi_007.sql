{% set new_scorecard_kpi_007 %}

  select 
    company_code, 
    fiscyearper, 
    sum(revenue) as revenue, 
    round(sum(quantity)) as sales_volume, 
    sum(cogs) as cogs, 
    sum("mkt & commercial costs (6)") as mkt_commercial
  from food.marts_score_card.fi_scorecard
  where fiscyearper = '2023007' and company_code != ''
  group by company_code, fiscyearper

{% endset %}

{% set scorecard_kpi_007 %}

  select
    company::text as company_code,
    period::text as fiscyearper,
    revenue,
    sales_volume,
    cogs,
    mkt_commercial
  from food.nghia_dev.food_seed_scorecard__kpi_007
  
{% endset %}

{{
    audit_helper.compare_queries(
        a_query=new_scorecard_kpi_007,
        b_query=scorecard_kpi_007,
        primary_key="company_code, fiscyearper",
        summarize=true,
    )
}}