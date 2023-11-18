with misa_2022 as (

    select 
        *
    from {{ source("food_misa_22", "generalledger") }}
    where posteddate < '2023-01-01'

),

misa_2023 as (

    select 
        *
    from {{ source("food_misa", "generalledger") }}
    where posteddate >= '2023-01-01'

)

select * from misa_2022
union all
select * from misa_2023