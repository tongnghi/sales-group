with ke24 as (
    
    select
        ke24.sales_group,
        ke24.division,
        ke24.sales_office,
        ke24.material_group,
        ke24.dis_channel,
        ke24.customer,
        define_materialgroup.group as product_group,
        ke24.standard_cogs,
        ke24.factory_price,
        ke24.delivery_fee,
        ke24.billed_qty,
        ke24.increasing_price,
        ke24.decreasing_price,
        ke24.cktt_sauhd,
        ke24.cktt_sauhd_d_kg,
        ke24.discount_price,
        ke24.discount_price_d_kg,
        
       case when ke24.sales_group = '140' and ke24.division in ('10','20','30') then ke24.sales_group||'_'||ke24.division
            when ke24.sales_group in ('100','120','130','150','190') and ke24.division = '30' then ke24.sales_group||'_'||ke24.division
            when ke24.sales_group = '180' and ke24.sales_office = '1024' and ke24.division = '50' and ke24.dis_channel = '20' and ke24.material_group like 'A%%' and ke24.material_group like 'B%%' then ke24.sales_group||'_'||'140'
            when ke24.sales_group = '180' and ke24.sales_office = '1049' and ke24.division = '50' and ke24.dis_channel = '20' and ke24.material_group like 'A%%' and ke24.material_group like 'B%%' then ke24.sales_group||'_'||'180'
            when ke24.sales_group = '180' and ke24.sales_office = '1049' and ke24.division = '50' and ke24.dis_channel = '10' and ke24.customer = '101657' then ke24.sales_group||'_'||'QD'
            else ke24.sales_group end as sg,

        case when  ke24.dis_channel = '20' and ke24.division in ('10','20') then 'Internal_Feed'
            when  ke24.dis_channel = '30' then 'Internal_Farm' else 'Customer' end as customer_type,    

        nvl(mapping_typecktt.type_cktt,1) as type_cktt,

        case when type_cktt = 0 then 0
            else ke24.cktt_sauhd + ke24.cktt_sauhd_d_kg end as cktt_sauhd_congthem,
        
        cktt_sauhd_congthem + ke24.discount_price + ke24.discount_price_d_kg as cktt_congthem,

        case when salesgroup_ck.sales_group isnull then '0'
            else salesgroup_ck.sales_group  end as sales_group_loaitru

    from {{ ref("feed_stg_sap_ecc__ke24") }} ke24
    left join {{ ref("feed_stg_excel_pnl__mapping_ck") }} mapping_typecktt 
        on ke24.sales_group = mapping_typecktt.sales_group
        and ke24.sales_office = mapping_typecktt.sales_office
        and ke24.division = mapping_typecktt.division
    left join {{ ref("feed_stg_excel_pnl__mapping_salesgroup_ck") }} salesgroup_ck on ke24.sales_office = salesgroup_ck.sales_office
    left join {{ ref("feed_seed_pnl_mapping_define_materialgroup") }} define_materialgroup on ke24.material_group = define_materialgroup.material_group

)

select * from ke24 