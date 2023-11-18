with cast_bags as (

    select 
        product,
        cast(lpad(replace(perpost::varchar,'.',''),8,'0') as varchar) as perpost
    from {{ source("stg_excel_margin", "actual_bags") }} 

)

select * from cast_bags where perpost = '00082023'