with zfip0008 as (

    select * 
    from {{ ref("feed_stg_sap_ecc__zfip0008") }}
    where left(account,1) in ('5','6','7','8','9')

), 

combined_data as (
    
    select 
        cost_element,
        cost_center,
        net_off,
        seg1,
        seg3
        
    from {{ ref("feed_stg_sap_ecc__ksb1") }} ksb1
    where usnam = 'HO.GTPT'
    
    union all

    select 
        account as cost_element,
        cost_center,
        sum(ps_no) - sum(ps_co) as net_off,
        seg1,
        seg3

    from zfip0008
    group by account, cost_center,seg1, seg3

),

final as (

    select 
        combined_data.cost_element,
        combined_data.cost_center,
        combined_data.net_off,
        
        case when sales_group.sales_group isnull then sales_group2.sales_group
            else sales_group.sales_group end as sales_group,

        case when code_pnl.pnl_code isnull then code_pnl2.pnl_sap
            else code_pnl.pnl_code end as code_pnl

    from combined_data
    left join {{ ref("feed_seed_pnl_md_salesgroup_ut") }} sales_group on combined_data.cost_center = sales_group.cost_center
    left join {{ ref("feed_seed_pnl_md_salesgroup_ut2") }} sales_group2 on combined_data.seg1 = sales_group2.seg1 and combined_data.seg3 = sales_group2.seg3
    left join {{ ref("feed_seed_pnl_md_codepnl_ut") }} code_pnl on combined_data.cost_element = code_pnl.account and combined_data.cost_center = code_pnl.cost_center
    left join {{ ref("feed_seed_pnl_md_codepnl_ut2") }} code_pnl2 on combined_data.cost_element = code_pnl2.account

)

select
    sales_group,
    code_pnl,
    sum(net_off) as net_off
from final
group by sales_group, code_pnl
   