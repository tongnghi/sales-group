with pg as (
    select 
        *
    from {{ source("farm_excel_pnl", "farm_pg") }}
),

deduped as (
        {{
            dbt_utils.deduplicate(
                relation="pg",
                partition_by='pg',
                order_by="type asc",
            )
        }}
)

select * from deduped