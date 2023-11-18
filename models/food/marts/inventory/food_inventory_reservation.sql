{{
    config(
        materialized='incremental',
        unique_key= ['material','plant','storage_location','batch_number','at_date'],
        incremental_strategy="delete+insert"
    )
}}

select 
    matnr as material,
    werks as plant,
    lgort as storage_location,
    charg as batch_number,
    at_date,
    created_at,
    reserstk as reservation_stocks

from {{ ref('food_int_inventory__reservation_stocks') }}