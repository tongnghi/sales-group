with renamed as (
    
    select 
        "seg 2 " as seg2,
        industry
    
    from {{ source("farm_excel_pnl", "md_seg2_industry") }}
),

cast_s as (
    
    select 
        seg2::varchar,
        nvl(industry,'0') as industry
    from renamed

)
select * from cast_s
