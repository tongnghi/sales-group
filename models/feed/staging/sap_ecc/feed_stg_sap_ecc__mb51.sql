with mb51 as (

    select
        *,
        row_number() over (partition by erdat, ernam, tabix, perio) as dedup

    from {{ source("feed_sap_ecc","mb51") }}

),

period_latest as (

    select perio, max(erdat) as added_date
    from mb51
    group by perio

),

deduped as (

    select 
        mb51.*

    from period_latest
    left join mb51 on mb51.perio = period_latest.perio and mb51.erdat = period_latest.added_date
    where dedup = 1
    
),

renamed as (

    select 
        perio,
        werks as plant,
        matnr as material,
        bwart as movement_type,
        budat as posting_date,
        lifnr as vendor,
        erfmg as qty

    from deduped

)

select * from renamed where perio = '00082023'

