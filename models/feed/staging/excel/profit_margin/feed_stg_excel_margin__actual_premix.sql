with cast_premix as (
    select 
        lpad(replace(perpost::VARCHAR,'.',''),8,'0') as perpost,
        plant,
        product as ma_thanh_phan,
        price as giatrixacnhan
    from {{ source("stg_excel_margin", "actual_premix") }}

)

select * from cast_premix
where perpost = '00082023'