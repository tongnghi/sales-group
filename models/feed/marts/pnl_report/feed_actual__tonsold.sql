with cte1 as (

    select
        ke24.sg,
        ke24.customer_type,
        ke24.product_group,
        sum(ke24.billed_qty)/1000 as ton_sold

    from {{ ref("feed_int_pnl__ke24") }} ke24
    group by ke24.sg, ke24.customer_type, ke24.product_group

)

select * from cte1