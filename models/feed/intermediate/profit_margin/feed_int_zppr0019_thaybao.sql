---- trường hợp thay bao không tính theo BOM như ra thành phẩm C
with raw_data_filter as (

    --tạo filter để lọc theo đầu sp
    select 
        plant,
        material,
        ma_thanh_phan,
        sanluongyeucau_KH,
        sanluongyeucau_NK,
        sanluongxacnhan_DVcoban,
        giatrixacnhan,
        left(material,1) as productlevel_1,
        left(material,2) as productlevel_2,
        left(ma_thanh_phan,1) as subproductlevel

    from {{ ref("feed_stg_sap_ecc__zppr0019") }}
       
),

thaybao_C as (
    -- bỏ distinct
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

    from raw_data_filter
    where productlevel_1 ='C' and subproductlevel like 'C%%'

),

thaybao as (

    select 
        plant,
        material,
        ma_thanh_phan,
        sum(sanluongxacnhan_DVcoban) as sanluongxacnhan_DVcoban

    from thaybao_C
    group by  plant, material, ma_thanh_phan

)

select * from thaybao