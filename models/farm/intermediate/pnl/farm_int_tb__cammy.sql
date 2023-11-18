with cammy as (

    select * from {{ ref('feed_stg_sap_ecc__zfip0008') }}
    where left(account,1) in ('5','6','7','8') and profit_center like '201%%'

),

ke24 as (

    select
        sales_group,
        sum(primaterial_cost) as primaterial_cost,
        sum(packaging_cost) as packaging_cost,
        sum(labor_cost) as labor_cost,
        sum(machinery_cost) as machinery_cost,
        sum(electricity_cost) as electricity_cost,
        sum(steam_cost) as steam_cost,
        sum(equipment_cost) as equipment_cost,
        sum(overhead) as overhead
        
    from {{ ref("feed_stg_sap_ecc__ke24") }}
    where sales_group = '270'
    group by sales_group

),

unpivot_ke24 as (

    select 
        sales_group,
        cast('' as varchar) as subaccount,
        description,
        0 as starting_balance,
        0 as credit,
        0 as ending_balance,
        debit::numeric

    from ke24 
    unpivot (debit for description in (primaterial_cost , packaging_cost, labor_cost, machinery_cost, electricity_cost, steam_cost, equipment_cost, overhead))

),

union_tbcm as (

    select
        'KE24' as TB,
        account_tbcammy.account::varchar,
        unpivot_ke24.subaccount,
        unpivot_ke24.description,
        unpivot_ke24.starting_balance,
        unpivot_ke24.debit,
        unpivot_ke24.credit,
        unpivot_ke24.ending_balance,
        unpivot_ke24.debit - unpivot_ke24.credit as net_off

    from unpivot_ke24
    left join {{ ref("farm_seed_pnl_account_tbcammy") }} account_tbcammy on unpivot_ke24.description = account_tbcammy.descr

    union all

    select
        'CM' as TB,
        account,
        cost_center as subaccount,
        description,
        sum(_starting_balance) as starting_balance,
        sum(ps_no) as debit,
        sum(ps_co) as credit,
        sum(_ending_balance) as ending_balance,
        debit - credit as net_off

    from cammy 
    group by TB, account, subaccount, description

),

tbcm_draft as (

    select
        union_tbcm.*,

        case when sapaccount_descr.sap_account isnull then (case when gl_semenpnlcode.account isnull then 'Check' else  gl_semenpnlcode.semen_pnlcode end)
            else sapaccount_descr.descr end as pnl_code,

        case when right(union_tbcm.subaccount,3) = '929' then '929'
            when union_tbcm.subaccount = '' then '201'
            else left(union_tbcm.subaccount,3) end as sap_code

    from union_tbcm
    left join {{ ref("farm_seed_pnl_mapping_sapaccount_descr") }} sapaccount_descr on union_tbcm.account = sapaccount_descr.sap_account
    left join {{ ref("farm_seed_pnl_mapping_gl_semenpnlcode") }} gl_semenpnlcode on union_tbcm.account = gl_semenpnlcode.account
    where pnl_code <> 'Clear'
    -- những account clear sẽ k lấy

),

mapping_tbcm_draft as (

    select
        tbcm_draft.*,

        case when tbcm_draft.sap_code = '201' then 'Cam My'
            else '' end as mapping

    from tbcm_draft

),
 --cte này tạo do cột farm_name nhiều trường hợp blank nên k thể join
mapping_bu as (
    
    select
        farm_name,
        bu_name
        
    from {{ ref("farm_excel_stg_md_seg2_farm_name") }} 
    where farm_name <> ''

),

final_tb_cammy as (

    select 
        mapping_tbcm_draft.*,
        mapping_bu.bu_name as BU

    from mapping_tbcm_draft
    left join mapping_bu on mapping_tbcm_draft.mapping = mapping_bu.farm_name

)

select * from final_tb_cammy