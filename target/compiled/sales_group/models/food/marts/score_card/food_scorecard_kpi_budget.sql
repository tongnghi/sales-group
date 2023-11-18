select
    cat_code,
    cat_name,
    subcat1_code as subcat_1_code,
    subcat1_name as subcat_1_name,
    '' as subcat_2_code,
    '' as subcat_2_name,
    '' as "G/L Account",
    '' as expense_group_code,
    '' as expense_group_name,
    '' as company_code,
    case 
    when month is not null 
    then left(month,4) || right(month,2) || '01'
    end as posting_date,
    '' as channel_code,
    '' as channel_name,
    '' as matnr,
    '20' as value_type,
    month as fiscyearper,
        kpi_code,
        kpi_name,
    '' as pl_code, 
    '' as pl_name, 
    '' as pl_group_code, 
    '' as pl_group_name,
    '' as costcenter_code,
    '' as costcenter_name,
    '' as costcenter_full_name,
    '' as costcenter_group_code,
    '' as costcenter_group_name,
    0 as revenue,
    0 as quantity,
    0 as "revenue(D/KG)",
    0 as inter_profit,
    0 as cogs,
    0 as "cogs(+)",
    0 as "gross contribution (3) =  (1) - (+)",
    0 as "manufacturing cost (4)",
    0 as "gross margin (5) = (3) - (4)",
    0 as "mkt & commercial costs (6)",
    0 as "administrative costs (7)",
    0 as "chi phí tài chính(8)",
    0 as "thu nhập tài chính(9)",
    amount::numeric(38,2) as budget
from "food"."nghi_dev"."food_seed_scorecard_budget_2023"