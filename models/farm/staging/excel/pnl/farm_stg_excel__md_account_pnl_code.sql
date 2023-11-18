with renamed as (
    select 
        "account number" as account,
        description as descr,
        "swine_pnl code" as swine_pnl_code
    from {{ source("farm_excel_pnl", "md_account_pnl_code") }}
),

cast_pnl as (
    select
        account::varchar,
        descr,
        swine_pnl_code::varchar
    from renamed
)

select * from cast_pnl