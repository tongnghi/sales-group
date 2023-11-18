with renamed as (

    select * 
    from {{ source("feed_sap_ecc","mb5b") }}

),

period_latest as (

    select perio, max(erdat) as added_date
    from renamed
    group by perio

),

deduped as (

    select 
        renamed.*,
        row_number() over (partition by renamed.erdat, ernam, tabix, renamed.perio) as dedup
    from period_latest
    left join renamed on renamed.perio = period_latest.perio and renamed.erdat = period_latest.added_date
    
)

select
    perio,
    bwkey as plant,
    matnr as material,
    "start_date" as from_date,
    "end_date" as to_date,
    anfmenge as opening_stock,
    soll as total_receipt_qty,
    haben as total_issue_qty,
    endmenge as closing_stock,
    meins as unit_measure,
    anfwert as opening_value,
    sollwert as total_receipt_value,
    habenwert as total_issue_value,
    endwert as closing_value,
    waers as currency
    
from deduped
where dedup = 1 and perio = '00072023'

