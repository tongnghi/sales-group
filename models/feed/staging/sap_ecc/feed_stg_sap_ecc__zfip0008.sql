with fi08 as (
    
    select 
        *,
        row_number() over (partition by perio, racct, prctr, kostl, orderr) as dedup

    from {{ source("feed_sap_ecc","zfip0008") }}

),

renamed as (

    select 
        perio,
        racct, --cost element,
        prctr,
        kostl,
        "column006" as ps_no,
        "column007" as ps_co,
        "column003" as _starting_balance,
        "column010" as _ending_balance,
        racct_text as descrip

    from fi08
    where dedup = 1

),

cast_zfip0008 as (

    select 
        perio,
        cast(substring(racct,6,8) as varchar) as account,
        cast(substring(kostl,6,10) as varchar)  as cost_center,
        substring(prctr,6,7) as profit_center,
        ps_no,
        ps_co,
        _starting_balance,
        _ending_balance,
        left(cost_center,3) as seg1,
        substring(cost_center,6,2) as seg3,
        descrip,
        left(profit_center,3) as profitcenter_1,
        right(profit_center,2) as profitcenter_2,
        substring(profit_center,4,4) as mid_costcenter

    from renamed
)

select * from cast_zfip0008 where perio = '00082023'
