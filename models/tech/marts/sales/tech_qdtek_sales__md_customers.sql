with
    ultimate as (
        select
            branch as branch_code,
            "salse phụ trách" as pic_sales_name,
            "mã số thuế" as tax_code,
            "tên đầy đủ" as company_name,
            "khu vực" as business_aspects_name,
            "mức độ đánh giá khách hàng" as province_name,
            "loại khách hàng" as customer_type_name,
            "mã" as code,
            dept as department_code,
            team as team_code,
            "địa chỉ" as address_name,
            "thương hiệu" as brand_name
        from {{ source("qdtek_excel_sales", "customer") }}
    )

select *
from ultimate
