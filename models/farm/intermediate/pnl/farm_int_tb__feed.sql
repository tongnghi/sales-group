with feed as (

    select 
        ksb1.cost_element,
        ksb1.cost_center,
        ksb1.net_off,
        left(ksb1.cost_center,3) as seg1_1,
        substring(ksb1.cost_center,4,2) as seg2_1,
        substring(ksb1.cost_center,6,2) as seg3_1,
        right(ksb1.cost_center,3) as seg4_1,

        case when (seg1_1 = '180' or seg1_1 = '900') and seg3_1 = '00' then 'Farm_TQ'
            else '' end as PB,

        case when left(acc_sapslm.acc_slm,3) = '642' and PB = 'Farm_TQ' then '64293200'
            else acc_sapslm.acc_slm end as slm_account
        
    from {{ ref("feed_stg_sap_ecc__ksb1") }} ksb1
    left join {{ ref("farm_seed_pnl_accountsap_accountslm") }} acc_sapslm on ksb1.cost_element = acc_sapslm.acc_sap
    where ksb1.usnam = 'HO.GTPT'

),

feed_conti as (

    select
        feed.*,

        case when account_type.type_account isnull then 'Expenses'
            else account_type.type_account end as exp_fin,
        
        -- TODO: kiểm tra lại với user trường hợp subaccount khi mapping thì null => có cần lọc cost center, cost element không?
        case when (feed.cost_element = '94200800' and feed.cost_center = '1820200900') or (exp_fin = 'Finance' and feed.cost_center = '1820200900') then 'K3-90B-000-000'
            when  (feed.cost_element = '94200800' and feed.cost_center = '1810200900') or (exp_fin = 'Finance' and feed.cost_center = '1810200900') then 'K3-67B-000-000'
            else costcenter_sapslm.costcenter_slm end as subaccount,
        
        left(subaccount,2) as seg1,
        substring(subaccount,4,3) as seg2,
        substring(subaccount,8,3) as seg3,
        right(subaccount,3) as seg4,
        left(seg4,1) as seg4_2,
        seg1 || '-' || seg2 as seg1_seg2,
        seg4_2 || seg1_seg2 as seg412
        
    from feed
    left join {{ ref("farm_stg_excel_md_account_type") }} account_type on feed.slm_account = account_type.account
    left join {{ ref("farm_seed_pnl_costcentersap_slm") }} costcenter_sapslm on feed.cost_center = costcenter_sapslm.costcenter_sap
  
    -- TODO: kiểm tra lại với user trường hợp subaccount khi mapping thì null => có cần lọc cost center, cost element không?
),

allocate_feed as (

    select
        feed_conti.*,

        case when feed_conti.seg1_seg2 = 'D5-17B' then 'Func.Dept_SwineFarm'
            else  seg2_farmname.industry end as industry,

        case when (feed_conti.subaccount in ( 'K3-000-T01-000','K3-000-T02-000','K3-000-T07-000')) then 'Func.Dept_Swine Group'
            else (case when seg4_funcdept.seg412 isnull and seg1_allocate.seg1 is not null then seg1_allocate.allocate
                        when seg4_funcdept.seg412 is not null then seg4_funcdept.func_dept
                    else seg2_farmname.masterfile_name end) 
        end as allocate
 -- do master file có 1 trường hợp seg2 bằng blank nên khi load lên hệ thống bị trùng '000' với 1 seg2 khác
 
    from feed_conti
    left join {{ ref("farm_excel_stg_md_seg2_farm_name") }} seg2_farmname on feed_conti.seg2 = seg2_farmname.seg2  
    left join {{ ref("farm_stg_excel_md_seg4_funcdept") }} seg4_funcdept on feed_conti.seg412 = seg4_funcdept.seg412
    left join {{ ref("farm_stg_excel_md_seg1_allocate") }} seg1_allocate on feed_conti.seg1 = seg1_allocate.seg1

),

finacc_feed as (

    select
        allocate_feed.*,

        case when (allocate_feed.slm_account = '64293200' and subaccount in ('K3-67B-000-000','K3-90B-000-000','K3-000-000-P00')) then '64293200'
            when allocate_feed.exp_fin = 'Expense' then (case when allo_finacc.account isnull then allocate_feed.slm_account end)
            else allocate_feed.slm_account end as final_account

    from allocate_feed
    left join {{ ref("farm_stg_excel_md_allocate_final_account") }} allo_finacc on allocate_feed.allocate = allo_finacc.hofarm_allocate

),

pnl_code as (

    select
        finacc_feed.*,
        case when acc_pnlcode.swine_pnl_code isnull then '0' end as pnl_code

    from finacc_feed
    left join {{ ref("farm_stg_excel__md_account_pnl_code") }} acc_pnlcode on finacc_feed.final_account = acc_pnlcode.account
),

lookup_hoacc as (

    select
        *,

        case when pnl_code = '102' or pnl_code = '104' then slm_account   
            else '0' end as lookup_hoaccount
    -- else '' nhưng k chạy được do integer
    from pnl_code

),

tb_feed_final as (

    select
        lookup_hoacc.*,

        case when acc_pnlcode.swine_pnl_code isnull then 0 end as ho_account

    from lookup_hoacc
    left join {{ ref("farm_stg_excel__md_account_pnl_code") }} acc_pnlcode on lookup_hoacc.lookup_hoaccount = acc_pnlcode.account


)

select * from feed_conti

