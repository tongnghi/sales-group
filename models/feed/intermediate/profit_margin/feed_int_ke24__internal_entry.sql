with internal as (
    -- filter customer group = 40 from ke24
    select * from  {{ ref("feed_stg_sap_ecc__ke24") }}
    where sched_line_cat = 'Z3' and customer in ('1010','1020','1030','1040','1050','1060') and material like 'C%%'

),

total_qty as (

    select 
        plant, 
        material, 
        {# customer, #}
        sum(billed_qty)::numeric as qty
    from internal
    group by plant, material

)

select * from total_qty

