with renamed as (

    select 
        "seg 2" as seg2,
        nvl(industry,'0') as industry,
        nvl("masterfile name",'') as masterfile_name,
        nvl("farm name",'') as farm_name,
        nvl("bu name",'') as bu_name,
        nvl("farm type",'') as farm_type,
        nvl("poutry_farm type",'') as poutry_farm_type
    from {{ source("farm_excel_pnl", "md_seg2_farm_name") }}

),

cast_seg2 as (

    select 
        industry,
        masterfile_name,
        farm_name,
        bu_name,
        farm_type,
        poutry_farm_type,
        
        case when seg2 = '000' and masterfile_name = 'Func.Dept_LF Tax' then 'blank'
            when seg2 = '000' then '000'
            else seg2 end as seg2
        -- do master file có 1 trường hợp seg2 bằng blank nên khi load lên hệ thống bị trùng '000' với 1 seg2 khác
    from renamed

)
select * from cast_seg2