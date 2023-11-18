with renamed as (

    select 
        ordernbr as order_number,
        voucherid as voucher_id,
        voucheramt as voucher_amount,
        crtd_datetime as created_datetime,
        lupd_datetime,
        lupd_prog
    from {{ source("food_sol_gnf1001apprepl", "xt_safoweb_v") }}

)

select * from renamed