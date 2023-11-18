with fi_acdoca as (

    select 
        rbukrs,
        matnr,
        kalnr,
        qsprocess,
        werks,
        fiscyearper,
        sum(msl) as quantity,
        sum(quant1) as quant1

    from {{ ref('food_stg_sap_s4__fi_0fi_acdoca_10') }}
    where ktopl = 1000  and kokrs = 1000 and rldnr = '0L' and fiscyearper >= '2023001' and mlcateg = 'ZU' 
    group by rbukrs, kalnr, qsprocess, werks, fiscyearper, matnr
    
),

z8_fcml as (

    select
        kalnr_mat as kalnr,
        process as qsprocess,
        element,
        bdatj || poper as fiscyearper,
        eletxt,
        sum(prcdif_var) as prcdif_var,
        sum(prcdif_fix) as prcdif_fix

    from {{ ref('food_stg_sap_s4__tbl_fcml_ccs_r_all_v') }}
    where curtp = '10' and categ = 'ZU' and elesmhk = 'M0'
    group by kalnr_mat, process, element, fiscyearper, eletxt

),

cogs_by_component as (

    select
        fi_acdoca.*,
        z8_fcml.element,
        z8_fcml.eletxt,
        z8_fcml.prcdif_var,
        z8_fcml.prcdif_fix,
        z8_fcml.prcdif_var + z8_fcml.prcdif_fix as cogs_value

    from fi_acdoca
    left join z8_fcml using (kalnr, qsprocess, fiscyearper)

),

final as (

    select
        rbukrs as company_code,
        werks as plant,
        matnr as material,
        fiscyearper,
        element,
        eletxt,
        m_cogs.cogs_sub_type,
        m_cogs.cogs_type,
        sum(cogs_value) as cogs_value, 
        sum(quantity) as quantity,
        sum(quant1) as quant1

    from cogs_by_component
    left join {{ ref('food_seed_scorecard_mapping_cogs_type') }} m_cogs using (element)
    group by rbukrs, werks, matnr, fiscyearper, element, eletxt, m_cogs.cogs_sub_type, m_cogs.cogs_type

)

select
    final.*,
    md_matnr.gross_weight,
    md_matnr.net_weight,
    case when quantity != 0 then abs(cogs_value/quantity) else 0 end as cogs_unit_ea,
    case when md_matnr.gross_weight != 0 then cogs_unit_ea/md_matnr.gross_weight else cogs_unit_ea end as cogs_unit_kg

from final
left join {{ ref('food_stg_sap_s4__md_0material_attr') }} md_matnr on md_matnr.code = final.material