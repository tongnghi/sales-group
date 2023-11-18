{{ config(materialized="table") }}

select *
from {{ ref("farm_stg_sol__vsdwh_salesvolumed") }}
where
    (
        descr not ilike '%phân hữu cơ%'
        and descr not ilike '%mít%'
        and descr not ilike '%chuối%'
        and descr not ilike '%chuoi say%'
        and descr not ilike '%tinh dầu%'
        and descr not ilike '%tinh dau%'
        and descr not ilike '%hộp sả%'
    )
    and (
        left(account, 5) = '13684'
        or left(account, 3) = '511'
        or left(account, 8) = '71132000'
        or left(account, 8) = '63231421'
        or (
            left(account, 5) = '13682'
            and (upper(unitdesc) = 'CON' or upper(unitdesc) = 'HEAD')
        )
    )
