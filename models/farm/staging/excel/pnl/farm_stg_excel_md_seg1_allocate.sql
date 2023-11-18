with renamed as (
    select 
        description as descr,
        "seg 1" as seg1,
        allocate
    from {{ source("farm_excel_pnl", "md_seg1_allocate") }}
)

select * from renamed
where allocate is not null
--check lai với chị Phương xem có lấy từ hàng 15 xuống k???