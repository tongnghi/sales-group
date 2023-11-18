with tb_vnsemen as (

    select
        tb_farm.tb,
        tb_farm.account,
        tb_farm.subaccount,
        tb_farm.starting_balance,
        tb_farm.debit,
        tb_farm.credit,
        tb_farm.ending_balance,
        tb_farm.net_off,
        tb_farm.sub_act,
        tb_farm.type_account,
        tb_farm.seg1,
        tb_farm.seg2,
        tb_farm.seg3,
        tb_farm.seg4,
        tb_farm.seg4_2,
        tb_farm.seg1_seg2,
        tb_farm.incur,
        tb_farm.bs_pl,
        tb_farm.industry,
        tb_farm.allocate,
        tb_farm.final_account,
        account_semenpnlcode.semen_pnlcode as pnl_code,
        tb_farm.mapping,
        tb_farm.bu

    from {{ ref("farm_int_tb__unioned") }} tb_farm
    left join {{ ref("farm_seed_pnl_mapping_gl_semenpnlcode") }} account_semenpnlcode on tb_farm.final_account = account_semenpnlcode.account
    where seg2 in ('45B','58B') and seg4 in ('B00','B32','B33') and account_semenpnlcode.semen_pnlcode is not null and account_semenpnlcode.semen_pnlcode <> 'Clear'
    -- chỉ lấy 2 trại lên P&L là Hưng Yên và Bắc Kan dựa theo seg2 (45B, 58B) và seg4 (B00, B32,B33) và chỉ chọn account cần lên p&L dựa trên masterfile và bỏ clear
)

select * from tb_vnsemen
