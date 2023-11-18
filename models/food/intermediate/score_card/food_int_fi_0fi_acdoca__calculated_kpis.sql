with _0fi_acdoca_10_filter as (
    
    select
        racct as gl_account,
        ktopl,
        kokrs,
        rbukrs as company_code,
        rldnr,
        paph4_pa,
        paph5_pa,
        vkgrp_pa,
        budat as posting_date,
        matnr_copa,
        fiscyearper,
        rcntr as cost_center,
        awtyp,
        hsl,
        quant1,
        osl,
        kalnr as customer_code,
        qsprocess,
        kunnr,
        msl,
        werks as plant,

        vtweg,
        case when vtweg = '' then '98' else vtweg end as channel_code,
        matnr as material,
        case when matnr = '' then matnr_copa else matnr end as matnr_,

        right(paph4_pa, 2) as ph4_code,
        right(paph5_pa, 3) as ph5_code,
        -- account 0064180015 với doanh thu khác không có số lượng
        case when ((racct >= '0051100000' and racct <= '0051199999') or racct = '0064180015')
                    and channel_code != '70'
                then hsl
            else 0 end as _revenue, 

        case when (racct >= '0051100000' and racct <= '0051199999') and channel_code != '70'
                then quant1
            else 0 end as _volume,

        case when racct >= '0064200000' and racct <= '0064299999' 
                then hsl 
            else 0  end as admin_cost,

        case when racct >= '0064100000' and racct <= '0064199999' 
                then hsl 
            else 0 end as selling_cost

    from {{ ref("food_stg_sap_s4__fi_0fi_acdoca_10") }}
    where
        ktopl = 1000  -- chart of account 
        and kokrs = 1000
        and rldnr = '0L'  -- sob type
        and fiscyearper >= '2023001'

),

group_by_dims as (

    select
        case when cat.cate_code is not null then cat.cate_code
            when cat.cate_code is null and material != '' then '90' 
            else '99' end as cate_code,
        case when cat.cate_name is not null then cat.cate_name
            when cat.cate_name is null and material != '' then 'Others'
            else 'DT Khác' end as cate_name,
        gl_account,
        company_code,
        posting_date,
        channel_code,
        material,
        fiscyearper,
        cost_center,
        customer_code,
        qsprocess,
        plant,
        sum(msl) as quantity,
        sum(_revenue * (-1)) as revenue,
        sum(_volume * (-1)) as volume,
        sum(admin_cost) as admin_cost,
        sum(selling_cost) as selling_cost

    from _0fi_acdoca_10_filter
    left join {{ ref('food_seed_scorecard_mapping_ncate_code') }} cat using (ph5_code)
    group by
        gl_account,
        company_code,
        posting_date,
        channel_code,
        material,
        fiscyearper,
        cost_center,
        customer_code,
        qsprocess,
        plant,
        cat.cate_code,
        cat.cate_name

),

mapping_fields as (

    select 
        group_by_dims.*,
        pl_group.pl_code,
        pl_group.pl_name,
        pl_group.pl_group_code,
        staff_cost.dept,
        staff_cost.staff_cost_code
        
    from group_by_dims
    left join {{ ref('food_seed_scorecard_mapping_npl_group') }} pl_group using (gl_account)
    left join {{ ref('food_seed_scorecard_mapping_staff_cost') }} staff_cost using (cost_center)

),

applied_rules as (

    select 
        *,
        case when pl_group_code = '1' then 'Staff costs'
            when pl_group_code = '2' then 'Sales commissions'
            when pl_group_code = '3' then 'Travel Expenses'
            when pl_group_code = '4' then 'Marketing cost'
            when staff_cost_code = 'S2' then 'Retails' -- S2 exclude pl group 1
            when staff_cost_code = 'S5' then 'Warehouse'
            when pl_group_code = '8' then 'Depreciation'
            when pl_group_code = '7' then 'Repair & Maintenance'
            when pl_group_code = '5' then 'POP' -- 5 exclude pl group 1
                else 'Other selling costs'
        end as selling_cost_type,
        
        case when selling_cost_type = 'Staff costs' and dept is not null then dept
            when selling_cost_type = 'Staff costs' and dept is null then 'Others Selling'

            when selling_cost_type = 'Marketing cost' then 'Marketing cost'
            when selling_cost_type = 'Retails' and pl_group_code = '6' then 'Rental Meatshop'
            when selling_cost_type = 'Retails' and pl_group_code = '11' then 'Energy & utilities'
            when selling_cost_type = 'Retails' and pl_group_code = '7' then 'Repair & Maintenance'
            when selling_cost_type = 'Retails' and pl_group_code = '8' then 'Depreciations'
            when selling_cost_type = 'Retails' then 'Others'

            when selling_cost_type = 'Warehouse' and pl_group_code = '6' then 'Rent'
            when selling_cost_type = 'Warehouse' then 'Others'
        end as selling_cost_sub_type,

        case when pl_group_code = '20' then 'Staff costs'
            when pl_group_code = '21' then 'Office supplies & telecom'
            when pl_group_code = '24' then 'Office rent'
            when pl_group_code = '26' then 'Depreciation'
            when pl_group_code = '25' then 'R & M - Office Bldg & Eqpt' 
            when pl_group_code = '27' then 'Other Admin costs'
        end as admin_cost_type,

        case when admin_cost_type = 'Staff costs' and staff_cost_code is not null then dept
            when admin_cost_type = 'Staff costs' and staff_cost_code is null then 'Others admin'
        end as admin_cost_sub_type

    from mapping_fields

)

select 
    * 
from applied_rules

