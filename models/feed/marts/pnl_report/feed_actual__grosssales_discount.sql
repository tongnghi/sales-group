with ke24_1 as (

    select 
        sg,
        sales_group_loaitru,
        sum(cktt_sauhd_congthem) as  cktt_sauhd_congthem,
        sum(cktt_congthem) as  cktt_congthem,
        sum(factory_price) as  factory_price,
        sum(delivery_fee) as  delivery_fee
        
    from {{ ref("feed_int_pnl__ke24") }} 
    where sg in ('100','110','120','130','140','150','190','100_30','120_30','130_30','140_10','140_20','140_30','150_30','190_30')
    group by sg,sales_group_loaitru

),

zfip0008_1 as (

     select 
        sg,
        sum(net_off) as net_off

    from  {{ ref("feed_int_pnl__zfip0008") }}
    where pr in ('01','02') and code_pnl = '6' and sg is not null
    group by sg
    
),

dieu_chinh as (

    select 
        salesgroup as sales_group,
        sum(net_off) as dieuchinh

    from {{ ref("feed_int_pnl__ksb1") }} 
    where cost_element = '91511100' and code_pnl = '5'
    group by sales_group

),

heocaisua as (
    
    select
        salesgroup as sales_group,
        -sum(net_off) as heocaisua

    from {{ ref("feed_int_pnl__ksb1") }} 
    where cost_element = '91511200'
    group by sales_group

),

cte1 as (

    select
        ke24_1.sg as sales_group,

        sum(ke24_1.factory_price) + sum(ke24_1.delivery_fee) - nvl(sum(dieu_chinh.dieuchinh),0) as gross_sales,

        sum(ke24_1.cktt_congthem) + sum(zfip0008_1.net_off) - sum(ke24_1.cktt_sauhd_congthem)- nvl(sum(heocaisua.heocaisua),0) as discount

    from ke24_1
    left join zfip0008_1 on ke24_1.sg = zfip0008_1.sg 
    left join dieu_chinh on ke24_1.sg = dieu_chinh.sales_group
    left join heocaisua on ke24_1.sg = heocaisua.sales_group
    group by ke24_1.sg

),

ke24_2 as (

    select 
        sg,
        sum(cktt_sauhd_congthem) as  cktt_sauhd_congthem,
        sum(cktt_congthem) as  cktt_congthem,
        sum(factory_price) as  factory_price,
        sum(increasing_price) as  increasing_price,
        sum(decreasing_price) as  decreasing_price
        
    from {{ ref("feed_int_pnl__ke24") }} 
    where sg in ('280','290','330') or (sg = '310' and division = '91') or (sg = '180_QD' and sales_office = '1049' and division = '50' and dis_channel = '10' and customer = '101657')
    group by sg

),

zfip0008_2 as (

     select 
        sales_group,
        sum(net_off) as net_off

    from  {{ ref("feed_int_pnl__zfip0008") }}
    where code_pnl = '5' and sales_group is not null
    group by sales_group
    
),

ksb1_2 as (

    select 
       salesgroup as sales_group,
       sum(net_off) as net_off
    from {{ ref("feed_int_pnl__ksb1") }} 
    where code_pnl = '5' and salesgroup is not null
    group by sales_group

),

cte2 as (

    select 
        ke24_2.sg as sales_group,

        case when ke24_2.sg in('290','310','280') then sum(ke24_2.factory_price) + sum(ke24_2.increasing_price) + sum(ke24_2.decreasing_price) + sum(zfip0008_2.net_off) + sum(ksb1_2.net_off)
            when ke24_2.sg = '330' then sum(ksb1_2.net_off)
            when ke24_2.sg = '180_QD' then sum(ke24_2.factory_price) + sum(ke24_2.increasing_price) + sum(ke24_2.decreasing_price) + sum(zfip0008_2.net_off) + sum(ksb1_2.net_off)
        end as gross_sales,

        sum(ke24_2.cktt_congthem) + sum(zfip0008_1.net_off) - sum(ke24_2.cktt_sauhd_congthem)- nvl(sum(heocaisua.heocaisua),0) as discount

    from ke24_2
    left join zfip0008_2 on ke24_2.sg = zfip0008_2.sales_group
    left join zfip0008_1 on ke24_2.sg = zfip0008_1.sg
    left join ksb1_2 on ke24_2.sg = ksb1_2.sales_group
    left join heocaisua on ke24_2.sg = heocaisua.sales_group
    group by ke24_2.sg

),

ke24_3 as (

    select 
        sg,
        sum(standard_cogs) as standard_cogs,
        sum(cktt_sauhd_congthem) as  cktt_sauhd_congthem,
        sum(cktt_congthem) as  cktt_congthem,
        sum(factory_price) as  factory_price,
        sum(increasing_price) as  increasing_price,
        sum(decreasing_price) as  decreasing_price
        
    from {{ ref("feed_int_pnl__ke24") }} 
    where (sg in ('210','220','230','240','250','260') and division = '60') or (sg = '320' and division = '11')
    group by sg
    
),

zfip0008_3 as (

     select 
        sales_group,
        sum(net_off) as net_off

    from  {{ ref("feed_int_pnl__zfip0008") }}
    where 
    (sales_group = '210' and profit_center = '1014908' and cost_element = '51184101') 
    or (sales_group = '220' and profit_center = '1024808' and cost_element = '51184101')
    or (sales_group = '230' and profit_center = '1033708' and cost_element = '51184101') 
    or (sales_group = '240' and profit_center = '1042408' and cost_element = '51184101')
    or (sales_group = '250' and profit_center = '1052208' and cost_element = '51184101') 
    or (sales_group = '260' and profit_center = '1065708' and cost_element = '51184101') 
    or (sales_group = '320' and cost_element = '91511100' and cost_center = '1010224170')
    group by sales_group
    
),

ksb1_3 as (

    select 
       salesgroup as sales_group,
       sum(net_off) as net_off

    from {{ ref("feed_int_pnl__ksb1") }} 
    where 
    (sales_group = '210'  and cost_element = '51184101') 
    or (sales_group = '220'  and cost_element = '51184101')
    or (sales_group = '230'  and cost_element = '51184101') 
    or (sales_group = '240'  and cost_element = '51184101')
    or (sales_group = '250'  and cost_element = '51184101') 
    or (sales_group = '260'  and cost_element = '51184101') 
    or (sales_group = '320' and cost_element = '91511100' and cost_center = '1010224170')
    group by sales_group

-- do những acc này là acc FI nên không ghi nhận lên ksb1, tuy nhiên ksb1 có cột profit center (cần check lại). Nếu có trường hợp xảy ra thì lấy ra lọc
),

cte3 as (

    select 
        ke24_3.sg as sales_group,

        case when ke24_3.sg = '210' then sum(ke24_3.standard_cogs) + sum(zfip0008_3.net_off) + sum(ksb1_3.net_off)
            when ke24_3.sg = '220' then sum(ke24_3.standard_cogs) + sum(zfip0008_3.net_off) + sum(ksb1_3.net_off)
            when ke24_3.sg = '230' then sum(ke24_3.standard_cogs) + sum(zfip0008_3.net_off) + sum(ksb1_3.net_off)
            when ke24_3.sg = '240' then sum(ke24_3.standard_cogs) + sum(zfip0008_3.net_off) + sum(ksb1_3.net_off)
            when ke24_3.sg = '250' then sum(ke24_3.standard_cogs) + sum(zfip0008_3.net_off) + sum(ksb1_3.net_off)
            when ke24_3.sg = '260' then sum(ke24_3.standard_cogs) + sum(zfip0008_3.net_off) + sum(ksb1_3.net_off)
            when ke24_3.sg = '320' then sum(ke24_3.factory_price) + sum(ke24_3.increasing_price) + sum(ke24_3.decreasing_price) + sum(zfip0008_3.net_off) + sum(ksb1_3.net_off)
        end as gross_sales,

        sum(ke24_3.cktt_congthem) + sum(zfip0008_1.net_off) - sum(ke24_3.cktt_sauhd_congthem)- nvl(sum(heocaisua.heocaisua),0) as discount

    from ke24_3
    left join zfip0008_3 on ke24_3.sg = zfip0008_3.sales_group 
    left join zfip0008_1 on ke24_3.sg = zfip0008_1.sg
    left join ksb1_3 on ke24_3.sg = ksb1_3.sales_group
    left join heocaisua on ke24_3.sg = heocaisua.sales_group
    group by ke24_3.sg
    
),

ke24_4 as (

    select
        sg,
        sum(cktt_sauhd_congthem) as  cktt_sauhd_congthem,
        sum(cktt_congthem) as  cktt_congthem,
        sum(factory_price) as  factory_price,
        sum(increasing_price) as  increasing_price,
        sum(decreasing_price) as  decreasing_price
        
    from {{ ref("feed_int_pnl__ke24") }}  
    where sg in ('180_140','180_180')
    group by sg

),

cte4 as (
    
    select 
        ke24_4.sg as sales_group,

        case when ke24_4.sg in ('180_140','180_180') then sum(ke24_4.factory_price) + sum(ke24_4.increasing_price) + sum(ke24_4.decreasing_price) end as gross_sales,

        sum(ke24_4.cktt_congthem) + sum(zfip0008_1.net_off) - sum(ke24_4.cktt_sauhd_congthem)- nvl(sum(heocaisua.heocaisua),0) as discount

    from ke24_4
    left join zfip0008_1 on ke24_4.sg = zfip0008_1.sg
    left join heocaisua on ke24_4.sg = heocaisua.sales_group
    group by ke24_4.sg

),

final as (

    select
        *
    from cte1

    union all 

    select 
        *
    from cte2

    union all

    select 
        *
    from cte3

    union all

    select
        * 
    from cte4

)

select * from final