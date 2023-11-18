with ksb1 as (
    
    select 
        *,
        row_number() over (partition by erdat, ernam, tabix, perio, belnr) as dedup

    from {{ source("feed_sap_ecc","ksb1") }}

),

renamed as (

    select 
        perio,
        usnam,
        kstar as cost_element,
        kostl as cost_center,
        left(cost_center,3) as seg1,
        substring(cost_center,6,2) as seg3,
        sum(wrgbtr) as net_off

    from ksb1
    where dedup = 1
    group by perio,cost_element, cost_center, usnam
)

select * from renamed
where perio = '00082023'