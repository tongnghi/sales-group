with recal as (

    select 
        *,
        -- policy : max_date 
        case when manu_date is not null and psledbbd is null and shelf_life > 0 then manu_date + shelf_life 
            else psledbbd end as recal_expired_date,

        case when manu_date is not null and psledbbd is not null then psledbbd - manu_date
            else shelf_life end as recal_shelf_life,
        
        least(recal_shelf_life/3, 60) as warning_days
        
    from {{ ref('food_stg_sap_s4__mm_zds_mm_batch_attr') }}

),

attrs as (

    select
        *,
        ltrim(matnr,'0') as material,
        recal_expired_date - current_date as remain_days, 

        case when remain_days <= 0 then 'E'
            when remain_days < warning_days then 'W'
            else 'S' end batch_state

    from recal

)

select * from attrs
