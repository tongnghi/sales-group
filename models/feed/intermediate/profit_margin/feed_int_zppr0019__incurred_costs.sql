with material_p_tusx as (
    
    select * from {{ ref("feed_stg_excel_margin__actual_premix") }}

),

material_p as (

    select 
        plant,
        material,
        ma_thanh_phan,
        sanluongyeucau_KH,
        sanluongyeucau_NK,
        sanluongxacnhan_DVcoban,
        giatrixacnhan,
        productlevel_1,
        productlevel_2,
        subproductlevel

    from {{ ref("feed_stg_sap_ecc__zppr0019") }}
    where productlevel_1 ='P' and productlevel_2 != 'PI'

),

final_material_p as (

    select 
        material_p.plant,
        material_p.material,
        material_p.ma_thanh_phan,
        material_p.sanluongyeucau_KH,
        material_p.sanluongyeucau_NK,
        material_p.sanluongxacnhan_DVcoban,
        material_p.giatrixacnhan,

        coalesce(case when (material_p.plant = '1010' and left(material_p.ma_thanh_phan,2) = 'PI') then material_p_tusx.giatrixacnhan*material_p.sanluongxacnhan_DVcoban
                else material_p.giatrixacnhan end,0) as giatrixacnhan_final

    from material_p 
    left join material_p_tusx on material_p.plant = material_p_tusx.plant and material_p.ma_thanh_phan = material_p_tusx.ma_thanh_phan

),

group_material_p as (

    select   
        plant,
        material,
        sum(giatrixacnhan_final)/sum(sanluongxacnhan_DVcoban) as giabinhquan_P

    from final_material_p
    group by plant, material

),

material_x as (

    select 
        plant,
        material,
        ma_thanh_phan,
        sanluongyeucau_KH,
        sanluongyeucau_NK,
        sanluongxacnhan_DVcoban,
        giatrixacnhan,
        productlevel_1,
        productlevel_2,
        subproductlevel

    from {{ ref("feed_stg_sap_ecc__zppr0019") }}
    where productlevel_1 ='X' 

),

final_material_x as (
    
    select 
        material_x.plant,
        material_x.material,
        material_x.ma_thanh_phan,
        material_x.sanluongyeucau_KH,
        material_x.sanluongyeucau_NK,
        material_x.sanluongxacnhan_DVcoban,
        material_x.giatrixacnhan,

        coalesce(case when (left(material_x.ma_thanh_phan,1) = 'P') then group_material_p.giabinhquan_P*material_x.sanluongxacnhan_DVcoban
                else material_x.giatrixacnhan end,0) as giatrixacnhan_final

    from material_x 
    left join group_material_p  
    on material_x.plant = group_material_p.plant and material_x.ma_thanh_phan = group_material_p.material

),

group_material_x as (
    
    select   
        plant,
        material,
        sum(giatrixacnhan_final),sum(sanluongxacnhan_DVcoban), 
        sum(giatrixacnhan_final)/sum(sanluongxacnhan_DVcoban) as giabinhquan_X

    from final_material_x
    group by plant, material

),


material_c as (
    
    select 
        plant,
        material,
        ma_thanh_phan,
        sanluongyeucau_KH,
        sanluongyeucau_NK,
        sanluongxacnhan_DVcoban,
        giatrixacnhan,
        productlevel_1,
        productlevel_2,
        subproductlevel

    from {{ ref("feed_stg_sap_ecc__zppr0019") }}
    where productlevel_1 ='C' and subproductlevel not in ('B','C') 

),

final_material_c as (

    select 
        material_c.plant,
        material_c.material,
        material_c.ma_thanh_phan,
        material_c.sanluongyeucau_KH,
        material_c.sanluongyeucau_NK,
        material_c.sanluongxacnhan_DVcoban,
        material_c.giatrixacnhan,

        coalesce(case when (left(material_c.ma_thanh_phan,1) = 'T' or left(material_c.ma_thanh_phan,1) = 'C') then material_c.giatrixacnhan
                else group_material_x.giabinhquan_X*material_c.sanluongxacnhan_DVcoban end,0) as giatrixacnhan_final
            
    from material_c 
    left join group_material_x  
    on material_c.plant = group_material_x.plant and material_c.ma_thanh_phan = group_material_x.material

),

group_material_c as (

    select 
        plant,
        material,
        sum(giatrixacnhan_final) as giatrixn_final,
        sum(sanluongxacnhan_DVcoban) as sanluongxacnhan_DVcoban_final

    from final_material_c
    group by plant, material

),

material_b as (

    select 
        plant,
        material,
        ma_thanh_phan,
        sanluongyeucau_KH,
        sanluongyeucau_NK,
        sanluongxacnhan_DVcoban,
        giatrixacnhan,
        productlevel_1,
        productlevel_2,
        subproductlevel

    from {{ ref("feed_stg_sap_ecc__zppr0019") }}
    where productlevel_1 ='C' and ma_thanh_phan like 'B%%'

),

mabaobi_doichieu as (

    select 
        product
    from {{ ref("feed_stg_excel_margin__actual_bags") }}

),

final_material_b as (

    select 
        material_b.plant,
        material_b.material,
        material_b.ma_thanh_phan,
        material_b.sanluongyeucau_KH,
        material_b.sanluongyeucau_NK,
        material_b.sanluongxacnhan_DVcoban,
        material_b.giatrixacnhan,
        material_b.giatrixacnhan as giatrixacnhan_final,

        case when (mabaobi_doichieu.product is not null) then  0
                else material_b.sanluongxacnhan_DVcoban end as sanluong
            
    from material_b 
    left join mabaobi_doichieu 
    on material_b.ma_thanh_phan = mabaobi_doichieu.product

),

group_material_b as (
        
    select 
        plant,
        material,
        ma_thanh_phan,
        sum(giatrixacnhan_final) as giatrixn_final ,
        sum(sanluong) as sanluong_final

    from final_material_b
    group by plant, material,ma_thanh_phan

),

pivot_final_material_b as (
        
    select 
        plant,
        material,
        sanluong_final as sl,
        giatrixn_final as gt,

        case 
            when sl = 0 then 0
            when (right(material,2) = '01') then gt/sl/40
            when (right(material,3) = '600') then gt/sl/600
            else gt/sl/right(material,2)::float 
        end as giabaobi            

    from group_material_b
    
),

pivot_final_material_c as (
        
    select  
        group_material_c.plant,
        group_material_c.material,
        sum(group_material_c.sanluongxacnhan_DVcoban_final) as sanluongxacnhan_DVcoban_final ,
        sum(group_material_c.giatrixn_final) as giatrixn_final,

        case when sum(group_material_c.sanluongxacnhan_DVcoban_final) = 0 then 0 
            else sum(group_material_c.giatrixn_final)/sum(group_material_c.sanluongxacnhan_DVcoban_final)
        end as raw_gia_version,

        case when sum(pivot_final_material_b.giabaobi) = 0 then 0
            else sum(pivot_final_material_b.giabaobi) end as baobi

    from group_material_c 
    left join pivot_final_material_b  
    on group_material_c.plant = pivot_final_material_b.plant and group_material_c.material = pivot_final_material_b.material
    group by group_material_c.plant, group_material_c.material

)

select * from pivot_final_material_c

--TODO (hien tai khong can tinh margin):
-- trường hợp PI tính riêng như 1 thành phẩm
{# material_PI as (
        select distinct
                plant,
                material,
                ma_thanh_phan,
                sanluongyeucau_KH,
                sanluongyeucau_NK,
                sanluongxacnhan_DVcoban,
                giatrixacnhan,
                productlevel_1,
                productlevel_2,
                subproductlevel
                case when subproductlevel = 'B' then "Bag"
                        else "Raw" end as Type 
        from {{ ref("feed_stg_sap_ecc_zppr0019") }}
        where productlevel_2 ='PI' 

) #}