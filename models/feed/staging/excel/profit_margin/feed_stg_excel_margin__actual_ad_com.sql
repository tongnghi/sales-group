with renamed as (
    select
        perpost,
        plant,
        "sale group" as sales_group,
        "dis.channel" as dis_channel,
        division,
        "sale office" as sales_office,
        "material group" as material_group,
        "commer_varcost_ dich danh" as commer_vardichdanh,
        "commer_varcost_k dich danh" as commer_varkdichdanh,
        "com variable" as com_var,
        "finan variable" as fin_var,
        "commer_fixcost_ dich danh" as commer_fixdichdanh,
        "commer_fixcost_ k dich danh" as commer_fixkdichdanh,
        "com variable_fix" as com_varfix,
        "admin fixed" as admin_fixed,
        "revfx đ/kg" as rev_dkg
from feed.stg_excel_margin.actual_ad_com
),

cast_adcom as (
    select
        lpad(replace(perpost::VARCHAR,'.',''),8,'0') as perpost,
        plant::varchar,
        sales_group::varchar,
        dis_channel::varchar,
        sales_office::varchar,
        division::varchar,
        material_group::varchar,
        commer_vardichdanh::float,
        commer_varkdichdanh::float,
        com_var::float,
        fin_var::float,
        commer_fixdichdanh::float,
        commer_fixkdichdanh::float,
        com_varfix::float,
        admin_fixed::float,
        rev_dkg::float 
    from renamed
)
select *
from cast_adcom
where perpost = '00082023'
