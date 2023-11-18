with ex_cogs as (

    select 
        fiscyearper,
        company_code,
        plant,
        ltrim(material,'0') as material,
        cate_name,
        cate_code,
        channel_code,
        customer_code,
        posting_date,
        gl_account,
        cost_center,
        qsprocess,
        revenue,
        volume,
        admin_cost,
        selling_cost,
        selling_cost_type,
        selling_cost_sub_type,
        admin_cost_type,
        admin_cost_sub_type,
        pl_code,
        pl_name,
        pl_group_code,
        dept,
        staff_cost_code,
        0 as cogs

    from {{ ref('food_int_fi_0fi_acdoca__calculated_kpis') }}

)

{# _cogs as (

    select 
        fiscyearper, 
        company_code, 
        plant, 
        material, 
        cate_name, 
        cate_code, 
        channel_code, 
        customer_code,
        posting_date,
        null as gl_account,
        null as cost_center,
        null as qsprocess,
        0 as revenue,
        0 as volume,
        0 as admin_cost,
        null as selling_cost,
        null as selling_cost_type,
        null as selling_cost_sub_type,
        null as admin_cost_type,
        null as admin_cost_sub_type,
        null as pl_code,
        null as pl_name,
        null as pl_group_code,
        null as dept,
        null as staff_cost_code,
        sum(cogs_value) as cogs

    from {{ ref('food_scorecard_cogs') }}
    group by 
        fiscyearper, 
        company_code, 
        plant, 
        material, 
        cate_name, 
        cate_code, 
        channel_code, 
        customer_code,
        posting_date
),

final as (

    select * from ex_cogs
    union all 
    select * from _cogs

)

select * from final #}

select * from ex_cogs
