with renamed as (
    select 
        "ho farm allocate" as hofarm_allocate,
        account
    from {{ source("farm_excel_pnl", "md_allocate_final_account") }}
),

cast_allocate as (
    select
        account::varchar,
        hofarm_allocate
    from renamed
)

select * from cast_allocate