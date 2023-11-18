select
    matnr,
    werks,
    lgort,
    charg,
    sum(lfimg) as reserstk,
    current_date as at_date,
    current_timestamp as created_at

from {{ ref('food_stg_sap_s4__sd_2lis_12_vcitm') }}
where erdat::date >= (getdate() + interval '7 hour' - interval '31 day')
    and wbsta != 'C'
group by matnr, werks, lgort, charg