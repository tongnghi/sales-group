
{{
    config(
        materialized="table",
    )
}}

with source as (

    select 
        charg,
        matnr,
        batch_id,
        batch_type,
        deact_bm ,-- Batch is no longer active
        hsdat::date, -- manufacture_date
        iprkz, -- Period Indicator for Shelf Life Expiration Date
        licha, -- Supplier Batch Number,
        lifnr, -- Vendor's account number
        lvorm, -- Deletion Flag for All Data in a Batch
        lwedt, -- Date of last goods receipt
        mhdhb::int, -- Total shelf life
        mhdrz, --minimum remaining shelf life
        ntgew, --net weight
        qndat, --next inspection date
        sgt_scat, --stock segment
        verab, --availability date
        vfdat::date, --shelf life expiration or best-before date
        voleh, --volume unit
        volum, --volume
        zaedt, --date of last status change
        zfdat, --date of certification
        zusch, --batch status key
        laeda, -- Date of Last Change
        zustd,
        
        row_number() over (partition by charg, matnr order by vfdat desc) as dedup 

    from {{ source("food_sap_s4", "mm_zds_mm_batch_attr") }}

)

select 
    charg,
    matnr,
    batch_type,
    lvorm as batch_flag,
    zaedt as last_change,
    laeda as changed_on,
    verab as valid_from,
    vfdat as psledbbd, -- hsd (expired date)
    zustd as batch_restricted,
    lwedt as last_receipt,
    qndat as inspection_date,
    hsdat as manu_date,
    zfdat as certified_on,
    iprkz as period_sled,
    mhdrz as remain_life,
    mhdhb as shelf_life
from source
where dedup = 1