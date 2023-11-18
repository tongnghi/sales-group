

  create view "food"."nghi_dev"."food_stg_sap_s4__fi_0fi_acdoca_10__dbt_tmp" as (
    

with source as (

    

    select *
    from "food"."stg_sap_s4"."fi_0fi_acdoca_10"
    where
        
            budat >= to_char(current_date - interval '1 day', 'YYYYMMDD')
        
        
        
),

deduped as (
    with row_numbered as (
        select
            _inner.*,
            row_number() over (
                partition by rclnt, rldnr, rbukrs, gjahr, belnr, docln
                order by _created_at desc
            ) as rn
        from source as _inner
    )

    select
        distinct data.*
    from source as data
    
    natural join row_numbered
    where row_numbered.rn = 1
)

select * from deduped
  ) with no schema binding;
