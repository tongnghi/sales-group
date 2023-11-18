with renamed as (
    select 
        "type account" as type_account,
        "account number" as account
    from {{ source("farm_excel_pnl", "md_account_type") }}
),

cast_type as (
    select
        account::varchar,
        type_account::varchar
    from renamed
)

select * from cast_type