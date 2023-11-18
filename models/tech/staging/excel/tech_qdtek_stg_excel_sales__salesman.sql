select 
    "mã nv _tên nv"::text,
    "họ và tên"::text as sales_name,
    team::text as team_code,
    "phòng"::text as department_code,
    "mã nv cũ "::text as old_sales_code,
    mnv::text as new_sales_code,
    "ngành"::text as branch_code,
    "remark 2023"::text
from {{ source("qdtek_excel_sales", "salesman") }}