with ton_sold as (
    select 
        ke25.sales_group,
        ke25.material_group,
        --customergroup
        define_materialgroup.group,
        sum(ke25.billed_qty) as ton_sold
    from {{ ref("feed_stg_sap_ecc__ke25") }} ke25
    left join {{ ref("feed_seed_pnl_mapping_define_materialgroup") }} define_materialgroup on ke25.material_group = define_materialgroup.material_group
    group by ke25.sales_group, define_materialgroup.group, ke25.material_group
)

select * from ton_sold