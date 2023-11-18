select
    fi.cat_code,
    fi.cat_name,
    fi.subcat_1_code,
    fi.subcat_1_name,
    fi.subcat_2_code,
    fi.subcat_2_name,
    fi."G/L Account",
    case
        when fi."G/L Account" >= '0064100000' and fi."G/L Account" <= '0064199999' 
        then '01'
        when fi."G/L Account" >= '0064200000' and fi."G/L Account" <= '0064299999' 
        then '02'
        when (fi."G/L Account" >= '0062200000' and fi."G/L Account" <= '0062299999')
        or  (fi."G/L Account" >= '0062700000' and fi."G/L Account" <= '0062799999')
        then '03'
    end as expense_group_code,

    case when expense_group_code = '01'
        then 'CP bán hàng'
        when  expense_group_code = '02'
        then 'CP quản lý'
        when  expense_group_code = '03'
        then 'CP sản xuất'
    end as expense_group_name,
    fi.company_code,
    fi.posting_date,
    fi.channel_code,
    case when fi.channel_code = '98'
    then 'Khác'
    else cn.name end as channel_name,
    fi.matnr,
    '10' as value_type, 
    fi.fiscyearper,
    '' as kpi_code,
    '' as kpi_name,
    pl.pl_code, 
    pl.pl_name, 
    pl.pl_group_code, 
    pl.pl_group_name,
    fi.costcenter_code,
    ctxt.short_name as costcenter_name,
    ctxt.medium_name as costcenter_full_name,
    cc.costcenter_group_code,
    cc.costcenter_group_name,
    clh.hier_cost_element_name,
    clh.cost_element_code,
    fi.customer_code,
    fi.plant,
    fi.revenue,
    fi.quantity,
    fi."revenue(D/KG)",
    fi.cogs,
    fi."cogs(+)",
    fi."gross Contribution (3) =  (1) - (+)",
    fi."manufacturing cost (4)",
    fi."gross margin (5) = (3) - (4)",
    fi."mkt & Commercial Costs (6)",
    fi."administrative costs (7)",
    fi."chi phí tài chính(8)",
    fi."thu nhập tài chính(9)",
    fi."10-raw material",
    fi."20-packaging",
    fi."30-subcontract",
    fi."40-delivery cost sto",
    fi."51-emp.outsourcing",
    fi."52-consum&supplies",
    fi."53-energy& utilities",
    fi."59-other expenses",
    fi."61-wages",
    fi."62-other wages",
    fi."63-depreciation exp",
    fi."69-other expenses",
    fi."80-merchendise",
    fi."90-inter.profit"

from "food"."nghi_dev"."food_int_fi_0fi_acdoca_10__filtered_to_revenue" fi
left join "food"."nghi_dev"."food_stg_sap_s4__md_0distr_chan_text" cn 
on fi.channel_code = cn.code

left join "food"."nghi_dev"."food_mapping_pl_plgroup" pl
on fi."G/L Account" = pl."G/L Account"

left join "food"."nghi_dev"."food_mapping_costcenter_costcentergroup" cc
on fi.costcenter_code = cc.costcenter_code

left join "food"."nghi_dev"."food_stg_sap_s4__md_0costcenter_text" ctxt
on fi.costcenter_code = ctxt.code

left join "food"."nghi_dev"."food_mapping_hierarchies_costelement" clh
on fi."G/L Account" = clh.cost_element_code

union all

select
    cat_code,
    cat_name,
    subcat_1_code,
    subcat_1_name,
    subcat_2_code,
    subcat_2_name,
    "G/L Account",
    expense_group_code,
    expense_group_name,
    company_code,
    posting_date,
    channel_code,
    channel_name,
    matnr,
    value_type, 
    fiscyearper,
    kpi_code,
    kpi_name,
    pl_code, 
    pl_name, 
    pl_group_code, 
    pl_group_name,
    costcenter_code,
    costcenter_name,
    costcenter_full_name,
    costcenter_group_code,
    costcenter_group_name,
    '' as hier_cost_element_name,
    '' as cost_element_code,
    '' as customer_code,
    '' as plant,
    budget * 1000 as revenue, 
    quantity,
    "revenue(D/KG)",
    cogs,
    "cogs(+)",
    "gross Contribution (3) =  (1) - (+)",
    "manufacturing cost (4)",
    "gross margin (5) = (3) - (4)",
    "mkt & Commercial Costs (6)",
    "administrative costs (7)",
    "chi phí tài chính(8)",
    "thu nhập tài chính(9)",
    0 as "10-raw material",
    0 as "20-packaging",
    0 as "30-subcontract",
    0 as "40-delivery cost sto",
    0 as "51-emp.outsourcing",
    0 as "52-consum&supplies",
    0 as "53-energy& utilities",
    0 as "59-other expenses",
    0 as "61-wages",
    0 as "62-other wages",
    0 as "63-depreciation exp",
    0 as "69-other expenses",
    0 as "80-merchendise",
    0 as "90-inter.profit"

from "food"."nghi_dev"."kpi_budget"