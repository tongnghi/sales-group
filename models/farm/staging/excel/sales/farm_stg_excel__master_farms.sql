select
    farmid,
    farmname,
    f_type as ftype,
    buid,
    buname,
    farm_sub_code as sub,
    regionid,
    case when compcode is null then '9999' else compcode end as compcode
from {{ source("farm_excel_sales", "master_data_farm_info") }}
