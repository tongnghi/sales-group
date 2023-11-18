with tb_unioned_slm as (

    {{ dbt_utils.union_relations(

        relations=[ ref("farm_stg_sol_fasiaapp_xp_01610ab_accthist_erp_final") , 
                    ref("farm_stg_sol_arwblapp_xp_01610ab_accthist_erp_final"), 
                    ref("farm_stg_sol_mtr2002app_xp_01610ab_accthist_erp_final"),
                    ref("farm_stg_sol_dnb2001app_xp_01610ab_accthist_erp_final"),
                    ref("farm_stg_sol_arwhyapp_xp_01610ab_accthist_erp_final"),
                    ref("farm_stg_sol_mtr1001app_xp_01610ab_accthist_erp_final"),
                    ref("farm_stg_sol_mtr2001app_xp_01610ab_accthist_erp_final"),
                    ref("farm_stg_sol_bsh1001app_xp_01610ab_accthist_erp_final")]

    ) }}

),

selected_fields_slm as (
    
    select
        begpernbr,
        endpernbr,
        fiscyr,
        periodactivityneg,
        periodactivitypos,
        TRIM(acct)::varchar as account,
        TRIM(sub)::varchar as subaccount,
        right(subaccount,3) as seg4_draft,
        cpnyid,
        legal,
        legal_name,
        ledgerid,
        balancetype,
        dramttot as debit,
        cramttot as credit,
        startingbalance as starting_balance,
        begbal,
        endingbalance as ending_balance
      
    from tb_unioned_slm
    where begpernbr = '202307'

),

sap_tbcammy as (

    select * from {{ ref('feed_stg_sap_ecc__zfip0008') }}
    where left(account,1) in ('5','6','7','8') and profit_center like '201%%'

),

selected_fields_sapcm as (

    select
        account,
        cast('B103B000B00' as varchar) as subaccount, 
        -- CM chuyển về 1 subact khi đưa vào TB FARM để lên cost structure
        _starting_balance as starting_balance,
        ps_no as debit,
        ps_co as credit,
        _ending_balance as ending_balance,
        right(subaccount,3) as seg4_draft

    from sap_tbcammy

),

tb_farm_unioned as (

    select 
        account,
        subaccount::varchar,
        starting_balance,
        debit,
        credit,
        ending_balance,
        right(subaccount,3) as seg4_draft,

        case when legal = 'FASIAAPP' then 'ASIA'
                when legal = 'MTR2002APP' then 'DLK'
                when legal = 'MTR2001APP' then 'LFBT'
                when legal = 'MTR1001APP' then 'LFBD'
                when legal = 'DNB2001APP' then 'DNB1'
                when legal = 'ARWHYAPP' then 'HY'
                when legal = 'BSH1001SYS' then 'TN'
                when legal = 'ARWBL' then 'DN'
        end as TB        
        
    from selected_fields_slm 
    
    union all

    select
        account,
        subaccount::varchar,
        starting_balance,
        debit,
        credit,
        ending_balance,
        'CM' as TB,
        right(subaccount,3) as seg4_draft

    from selected_fields_sapcm
    
),

tb_farm_draft as (

    select
        tb_farm_unioned.*,
        tb_farm_unioned.debit - tb_farm_unioned.credit as net_off,

        case when sub_seg4.sub isnull then tb_farm_unioned.subaccount
            else left(tb_farm_unioned.subaccount,8)  || sub_seg4.sub end as sub_act,

        case when account_type.type_account isnull then '0'
            else account_type.type_account end as type_account,

        left(sub_act,2) as seg1,
        substring(sub_act,3,3) as seg2,
        substring(sub_act,6,3) as seg3,
        right(sub_act,3) as seg4,
        left(right(sub_act,3),1) as seg4_2,
        seg1 || '-' || seg2 as seg1_seg2,
        seg4_2 || seg1_seg2 as seg412,

        case when net_off = 0 then '0'
            else 'x' end as incur,

        case when left(tb_farm_unioned.account,1) in ('5','6','7','8') then 'PL'
            else 'BS' end as bs_pl

    from tb_farm_unioned
    left join {{ ref("farm_stg_excel_md_subaccount_seg4") }} sub_seg4 on tb_farm_unioned.seg4_draft = sub_seg4.seg4
    left join {{ ref("farm_stg_excel_md_account_type") }} account_type on tb_farm_unioned.account = account_type.account

),

tb_farm as (

    select 
        tb_farm_draft.TB,
        tb_farm_draft.account,
        tb_farm_draft.subaccount,
        tb_farm_draft.starting_balance,
        tb_farm_draft.debit,
        tb_farm_draft.credit,
        tb_farm_draft.ending_balance,
        tb_farm_draft.net_off,
        tb_farm_draft.sub_act,
        tb_farm_draft.type_account,
        tb_farm_draft.seg1,
        tb_farm_draft.seg2,
        tb_farm_draft.seg3,
        tb_farm_draft.seg4,
        tb_farm_draft.seg4_2,
        tb_farm_draft.seg1_seg2,
        tb_farm_draft.incur,
        tb_farm_draft.bs_pl,
        
        case when tb_farm_draft.seg1_seg2 = 'D5-17B' then 'Func.Dept_SwineFarm'
            else seg2_farmname.industry end as industry,

        case when account_bu.account is not null then  nvl(account_bu.allocate,'')
            else nvl(subact_bu.allocate,'') end as allocate
         -- do master file có 1 trường hợp seg2 bằng blank nên khi load lên hệ thống bị trùng '000' với 1 seg2 khác
       
    from tb_farm_draft
    left join {{ ref("farm_excel_stg_md_seg2_farm_name") }} seg2_farmname on tb_farm_draft.seg2 = seg2_farmname.seg2  
    left join {{ ref("farm_seed_pnl_md_account_bu")}} account_bu on tb_farm_draft.account = account_bu.account and tb_farm_draft.sub_act = account_bu.sub_act and tb_farm_draft.bs_pl = account_bu.bs_pl
    left join {{ ref("farm_seed_pnl_md_subact_bu")}} subact_bu on tb_farm_draft.sub_act = subact_bu.sub_act and tb_farm_draft.bs_pl = subact_bu.bs_pl

),

final_account as (
    
    select
        tb_farm.*,

        case when tb_farm.account in ('64131001', '82120000', '82110000' ) then tb_farm.account
            else ( case when allocate_finalaccount.hofarm_allocate is not null then allocate_finalaccount.account
                        else tb_farm.account end)
        end as final_account

    from tb_farm
    left join {{ ref("farm_stg_excel_md_allocate_final_account") }} allocate_finalaccount on tb_farm.allocate = allocate_finalaccount.hofarm_allocate
    
),

pnl_code as (

    select 
        final_account.*,

        case when account_pnlcode.swine_pnl_code isnull then '0'
            else account_pnlcode.swine_pnl_code end as pnl_code,

        case when account_bu.account is not null then  nvl(account_bu.mapping,'')
            else nvl(subact_bu.mapping,'') end as mapping

    from final_account
    left join {{ ref("farm_stg_excel__md_account_pnl_code") }} account_pnlcode on final_account.final_account = account_pnlcode.account
    left join {{ ref("farm_seed_pnl_md_account_bu")}} account_bu on final_account.account = account_bu.account and final_account.sub_act = account_bu.sub_act and final_account.bs_pl = account_bu.bs_pl
    left join {{ ref("farm_seed_pnl_md_subact_bu")}} subact_bu on final_account.sub_act = subact_bu.sub_act and final_account.bs_pl = subact_bu.bs_pl

),

--cte này tạo do cột farm_name nhiều trường hợp blank nên k thể join
mapping_bu as (
    
    select
        farm_name,
        bu_name
        
    from {{ ref("farm_excel_stg_md_seg2_farm_name") }} 
    where farm_name <> ''

),

bu as (

    select
        pnl_code.*,

        case when account_bu.account is not null then  nvl(account_bu.bu,'')
            else nvl(subact_bu.bu,'') end as bu,

        case when pnl_code.pnl_code in ('102','104') then pnl_code.account 
            else '' end as lookup_hoaccount

    from pnl_code
    left join {{ ref("farm_seed_pnl_md_account_bu")}} account_bu on pnl_code.account = account_bu.account and pnl_code.sub_act = account_bu.sub_act and pnl_code.bs_pl = account_bu.bs_pl
    left join {{ ref("farm_seed_pnl_md_subact_bu")}} subact_bu on pnl_code.sub_act = subact_bu.sub_act and pnl_code.bs_pl = subact_bu.bs_pl

),

tb_farm_final as (

    select
        bu.*,

        case when left(bu.allocate,12) = 'PoultryFarm_' then seg2_farm_name.bu_name
            else '' end as poutry_north_south,

        case when left(bu.allocate,12) = 'PoultryFarm_' then seg2_farm_name.farm_type
            else '' end as pountry_leasing_outsource,

        case when account_pnlcode.swine_pnl_code isnull then 0 end as ho_account,

        case when bu.tb = 'HY' and bu.seg2 = '45B' and bu.seg4_2 = 'D' then 'Hung Yen'
            when bu.tb = 'HY' and bu.seg2 = '45B' and bu.seg4_2 = 'B' then 'Hung Yen Semen'
            when bu.tb = 'LFBD' and bu.seg2 = '58B' and bu.seg4_2 = 'D' then 'Bac Can 1'
            when bu.tb = 'LFBD' and bu.seg2 = '58B' and bu.seg4_2 = 'B' then 'Bac Can 1 Semen'
            when bu.tb = 'LFBD' and bu.seg2 = '59B' and bu.seg4_2 = 'D' then 'Yen Bai'
            when bu.tb = 'LFBD' and bu.seg2 = '58C' and bu.seg4_2 = 'D' then 'Bac Can 2'
            when bu.tb = 'LFBD' and bu.seg2 = '39B' and bu.seg4_2 = 'D' then 'Hoa Binh'
            when bu.tb = 'LFBD' and bu.seg2 = '48B' and bu.seg4_2 = 'D' then 'Thanh Hoa 2'
            when bu.tb = 'LFBD' and bu.seg2 = '48C' and bu.seg4_2 = 'D' then 'Thanh Hoa 1'
            when bu.tb = 'LFBD' and bu.seg2 = '48D' and bu.seg4_2 = 'D' then 'Thanh Hoa 3'
            when bu.tb = 'LFBD' and bu.seg2 = '26B' and bu.seg4_2 = 'D' then 'Gia Lai 1'
            when bu.tb = 'DLK' and bu.seg2 = '23B' and bu.seg4_2 = 'D' then 'Dak Lak 1'
            when bu.tb = 'LFBD' and bu.seg2 = '26C' and bu.seg4_2 = 'D' then 'Gia Lai 2'
            when bu.tb = 'LFBD' and bu.seg2 = '26D' and bu.seg4_2 = 'D' then 'Gia Lai 5'
            when bu.tb = 'LFBD' and bu.seg2 = '26E' and bu.seg4_2 = 'D' then 'Gia Lai 6'
            when bu.tb = 'LFBD' and bu.seg2 = '31B' and bu.seg4_2 = 'D' then 'Quang Nam 1'
            when bu.tb = 'LFBD' and bu.seg2 = '27B' and bu.seg4_2 = 'D' then 'Phu Yen'
            when bu.tb = 'ASIA' and bu.seg2 = '17B' and bu.seg4_2 = 'D' then 'Binh Thuan'
            when bu.tb = 'ASIA' and bu.seg2 = '19B' and bu.seg4_2 = 'D' then 'Dong Nam Bo 2'
            when bu.tb = 'DNB1' and bu.seg2 = '18C' and bu.seg4_2 = 'D' then 'Dong Nam Bo 1'
            when bu.tb = 'LFBT' and bu.seg2 = '88B' and bu.seg4_2 = 'D' then 'Lang Viet Nam'
            when bu.tb = 'LFBT' and bu.seg2 = '85B' and bu.seg4_2 = 'D' then 'Lang Viet 1'
            when bu.tb = 'LFBT' and bu.seg2 = '86B' and bu.seg4_2 = 'D' then 'Lang Viet 2'
            when bu.tb = 'ASIA' and bu.seg2 = '60B' and bu.seg4_2 = 'D' then 'Cujut'
            when bu.tb = 'LFBD' and bu.seg2 = '20B' and bu.seg4_2 = 'D' then 'Tay Ninh 1'
            when bu.tb = 'LFBD' and bu.seg2 = '20C' and bu.seg4_2 = 'D' then 'Tay Ninh 2'
            when bu.tb = 'LFBD' and bu.seg2 = '16B' and bu.seg4_2 = 'D' then 'Ca Mau 1'
            when bu.tb = 'LFBD' and bu.seg2 = '20D' and bu.seg4_2 = 'D' then 'Tay Ninh 3'
            when bu.tb = 'LFBD' and bu.seg2 = '20E' and bu.seg4_2 = 'D' then 'Tay Ninh 4'
            when bu.tb = 'LFBD' and bu.seg2 = '60C' and bu.seg4_2 = 'D' then 'Dak Nong 2'
            when bu.tb = 'LFBD' and bu.sub_act in ('D246R000P00','D269F021P00','D246RM21P00','D239C000P00','D244C000P00','D244D000P00','D244E000P00','D244G000P00','D244H000P00','D244J000P00','D244K000P00','D244M000P00','D244N000P00','D244O000P00','D244P000P00','D244Q000P00','D244S000P00','D246D000P00','D246S000P00','D247D000P00','D251D000P00','D252D000P00','D252E000P00','D252F000P00','D252T000P00','D252V000P00','D252W000P00','D252X000P00','D253S000P00','D262D000P00','D262E000P00','D262N000P00','D262T000P00','D262U000P00','D262V000P00','D262W000P00','D269C021P00','D269D021P00','D269G021P00','D244V000P00')
                then 'North Broiler'
            when bu.tb = 'LFBD' and bu.sub_act in ('D206P000P00','D267E000P00','D267E021P00','D206P000P70','D206G000P00','D206P000P20','D206P000P30','D204C000P00','D204D000P00','D204R000P00','D204S000P00','D204T000P00','D204V000P00','D204X000P00','D204Y000P00','D267C021P00','D267D021P00','D267C000P10','D204W000P00','D204Z000P10','D204W000P10','D204Z000P00','D220Z000P00')
                then 'South Broiler'
            when bu.tb = 'LFBD' and bu.sub_act in ('D217P000P70','D203P000P70','D202Q000P70','D213Q000P70','D217PM25P00','D219Q000P70','D219QM25P70','D203P000P30','D217P000P21','D217P000P22','D203P000P60','D217P000P20','D203P000P50','D217P000P50','D217PM26P00','D202QM25P50','D202QM25P70','D202QM26P70','D213QM25P51','D213QM26P70','D217P000P51','D217P000P52','D219QM25P50','D219QM25P52','D219QM26P70','D202QM25P51','D202QM25P52','D202QM26P50','D202QM26P52','D202QM26P60','D202QM26P61','D213QM26P61','D217PM25P50','D217PM25P52','D217PM26P50','D217PM26P52','D217PM26P60','D217PM26P62','D219QM25P51','D219QM26P50','D219QM26P52','D219QM26P60','D219QM26P62','D217PA04P70','D203P000P00','D203P000P20')
                then 'Breeder'
            when bu.tb = 'CM' then 'Cam My'
        end as coststructure_farmname,

        case when coststructure_farmname in ('Hung Yen','Hung Yen Semen','Bac Can 1','Bac Can 1 Semen','Yen Bai','Bac Can 2') then 'North 1'
            when coststructure_farmname in ('Hoa Binh','Thanh Hoa 2','Thanh Hoa 1','Thanh Hoa 3') then 'North 2'
            when coststructure_farmname in ('Gia Lai 1','Dak Lak 1','Gia Lai 2','Gia Lai 5','Gia Lai 6','Quang Nam 1','Phu Yen') then 'Central'
            when coststructure_farmname in ('Binh Thuan','Dong Nam Bo 2','Dong Nam Bo 1','Lang Viet Nam','Lang Viet 1','Lang Viet 2') then 'South 1'
            when coststructure_farmname in ('Cujut','Tay Ninh 1','Tay Ninh 2','Ca Mau 1','Tay Ninh 3','Tay Ninh 4','Dak Nong 2','Cam My') then 'South 2'
            when coststructure_farmname = 'North Broiler' then 'North-Broiler'
            when coststructure_farmname = 'South Broiler' then 'South-Broiler'
            when coststructure_farmname = 'Breeder' then 'South-Breeder'
        end as bu_type_2,


       case when bu_type_2 like 'North%%' then 'North'
            when bu_type_2 like 'South%%' then 'South'
            when bu_type_2 like 'Oversea' then 'Oversea'
            when bu_type_2 like 'Central' then 'Central'
        end as region_1,

        case when region_1 in ('Central','North') then 'North-Central'
            when region_1 = 'Oversea' then 'Oversea'
            when region_1 = 'South' then 'South'
        end as region_2,

        case when bu.seg4_2 = 'D' then 'Swine'
            when bu.seg4_2 = 'P' then 'Poultry'
        end as product_type

    from bu
    left join {{ ref("farm_excel_stg_md_seg2_farm_name") }} seg2_farm_name on bu.seg2 = seg2_farm_name.seg2
    left join {{ ref("farm_stg_excel__md_account_pnl_code") }} account_pnlcode on bu.lookup_hoaccount = account_pnlcode.account
)

select * from tb_farm_final 

