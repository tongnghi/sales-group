select
    "Số chứng từ"::text as billing_number,
    to_date("Ngày", 'DD-MM-YYYY') as billing_date,
    "Nghiệp vụ"::text as billing_type_name,
    "Nhóm khách hàng"::text as customer_group_name,
    "Mã hàng hóa"::text as article_code,
    upper(trim("Nhóm hàng hóa")) as article_group,
    "Tên nhóm hàng hóa"::text as article_group_name,
    "Mã nhóm hàng hóa"::text as article_group_code,
    (case when "Thành tiền TC" is null then 0 else "Thành tiền TC" end)::float as debit,
    (case when "Tiền C/K(VND)" is null then 0 else "Tiền C/K(VND)" end)::float
    as rebate,
    (
        case when "Thành tiền (trả) TC" is null then 0 else "Thành tiền (trả) TC" end
    )::float as credit,
    (
        case
            when "Giảm giá / chiếc khấu TC" is null
            then 0
            else "Giảm giá / chiếc khấu TC"
        end
    )::float as discount,
    (case when "Tăng giá TC" is null then 0 else "Tăng giá TC" end)::float as increase,
    "TT giá vốn (xuất) TC"::float as cogs_debit,
    "TT giá vốn (trả) TC"::float as cogs_credit,
    "Mã khách hàng"::text as customer_id,
    "Tên khách hàng"::text as customer_name,
    upper(trim("Mã nhân viên")) as salesman_id,
    "Tên nhân viên"::text as salesman_name
from {{ source("qdtek_excel_sales", "daily_sales") }}
