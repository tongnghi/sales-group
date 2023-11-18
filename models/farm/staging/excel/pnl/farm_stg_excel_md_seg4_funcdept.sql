with renamed as (
    select 
        "seg 1" as seg1,
        "seg 2" as seg2,
        "seg 4" as seg4,
        "func.dept" as func_dept,
        "seg 1-seg 2" as seg1_seg2,
        "seg 4_dau" as seg4_dau,
        seg4_dau || seg1_seg2 as seg412
    from {{ source("farm_excel_pnl", "md_seg4_funcdept") }}
)

select * from renamed