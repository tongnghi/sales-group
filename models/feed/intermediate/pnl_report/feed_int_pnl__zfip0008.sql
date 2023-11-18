with zfip0008 as (

     select 
        zfip0008.account as cost_element,
        zfip0008.cost_center,
        zfip0008.profit_center,
        zfip0008.ps_no - zfip0008.ps_co as net_off,
        zfip0008.seg1,
        zfip0008.seg3,
        zfip0008.profitcenter_2 as pr,

        case when sales_group.sales_group isnull then sales_group2.sales_group
            else sales_group.sales_group end as sales_group,

        case when code_pnl.pnl_code isnull then code_pnl2.pnl_sap
            else code_pnl.pnl_code end as code_pnl,

        case when zfip0008.cost_center = '' then profitcenter_salesgroup.sales_group_detail
            when  midcostcenter_salesgroup.sales_group = 'TS' then costcenter_salesgroup.sales_group
            else midcostcenter_salesgroup.sales_group
        end as sg

    from {{ ref("feed_stg_sap_ecc__zfip0008") }} zfip0008
    left join {{ ref("feed_seed_pnl_md_salesgroup_ut") }} sales_group on zfip0008.cost_center = sales_group.cost_center
    left join {{ ref("feed_seed_pnl_md_salesgroup_ut2") }} sales_group2 on zfip0008.seg1 = sales_group2.seg1 and zfip0008.seg3 = sales_group2.seg3
    left join {{ ref("feed_seed_pnl_md_codepnl_ut") }} code_pnl on zfip0008.account = code_pnl.account and zfip0008.cost_center = code_pnl.cost_center
    left join {{ ref("feed_seed_pnl_md_codepnl_ut2") }} code_pnl2 on zfip0008.account = code_pnl2.account
    left join {{ ref("feed_stg_excel_pnl__mapping_acount_pnl") }} acount_pnl on zfip0008.account = acount_pnl.account
    left join {{ ref("feed_seed_pnl_mapping_profitcenter_salesgroup") }} profitcenter_salesgroup 
        on zfip0008.profitcenter_1 = profitcenter_salesgroup.profitcenter_1
        and zfip0008.profitcenter_2 = profitcenter_salesgroup.profitcenter_2
    left join "feed"."vy_dev"."feed_seed_pnl_mapping_midcostcenter_salesgroup" midcostcenter_salesgroup
        on zfip0008.mid_costcenter = midcostcenter_salesgroup.mid_costcenter
    left join "feed"."vy_dev"."feed_seed_pnl_mapping_costcenter_salesgroup" costcenter_salesgroup
        on zfip0008.cost_center = costcenter_salesgroup.cost_center
    where left(zfip0008.account,1) in ('5','6','7','8','9')
)

select * from zfip0008