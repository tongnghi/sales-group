with ksb1 as (

    select 
        ksb1.cost_element,
        ksb1.cost_center,
        ksb1.net_off,
        ksb1.seg1,
        ksb1.seg3,
        
        case when sales_group.sales_group isnull then sales_group2.sales_group
            else sales_group.sales_group end as salesgroup,

        case when code_pnl.pnl_code isnull then code_pnl2.pnl_sap
            else code_pnl.pnl_code end as code_pnl

    from {{ ref("feed_stg_sap_ecc__ksb1") }} ksb1
    left join {{ ref("feed_seed_pnl_md_salesgroup_ut") }} sales_group on ksb1.cost_center = sales_group.cost_center
    left join {{ ref("feed_seed_pnl_md_salesgroup_ut2") }} sales_group2 on ksb1.seg1 = sales_group2.seg1 and ksb1.seg3 = sales_group2.seg3
    left join {{ ref("feed_seed_pnl_md_codepnl_ut") }} code_pnl on ksb1.cost_element = code_pnl.account and ksb1.cost_center = code_pnl.cost_center
    left join {{ ref("feed_seed_pnl_md_codepnl_ut2") }} code_pnl2 on ksb1.cost_element = code_pnl2.account
    where usnam = 'HO.GTPT' and salesgroup is not null and code_pnl is not null
    
)

select * from ksb1
