with _0fi_acdoca_10_filter as (
    
    select
        racct,
        ktopl,
        kokrs,
        rbukrs,
        rldnr,
        paph4_pa,
        paph5_pa,
        vkgrp_pa,
        matnr,
        budat,
        matnr_copa,
        vtweg,
        fiscyearper,
        rcntr,
        awtyp,
        hsl,
        quant1,
        osl,
        kalnr,
        qsprocess,
        kunnr,
        mlcateg,
        werks
    from {{ ref("food_stg_sap_s4__fi_0fi_acdoca_10") }}
    where
        ktopl = 1000  -- chart of account 
        and kokrs = 1000
        and rldnr = '0L'  -- sob type
        and fiscyearper >= '2023001'

),

mapping_cat_subcat as (

    select
        c.cat_code,
        c.cat_name,
        sc.subcat1_code,
        sc.subcat1_name,
        sc.subcat2_code,
        sc.subcat2_name,
        racct,
        ktopl,
        kokrs,
        rbukrs,
        rldnr,
        paph4_pa,
        paph5_pa,
        vkgrp_pa,
        matnr,
        matnr_copa,
        budat,
        vtweg,
        fiscyearper,
        rcntr,
        hsl,
        quant1,
        osl,
        awtyp,
        kunnr,
        kalnr,
        qsprocess,
        werks
    from _0fi_acdoca_10_filter
    left join {{ ref("food_seed_scorecard_mapping_categories") }} c
        on right(paph4_pa, 2) = c.ph4_code
    left join {{ ref("food_seed_scorecard_mapping_subcategories") }} sc
        on right(paph5_pa, 3) = sc.ph5_code

),

mapping_cat_subcat_special_case as (

    select
        cat_code,
        cat_name,
        subcat1_code,
        subcat1_name,
        subcat2_code,
        subcat2_name,
        racct,
        ktopl,
        kokrs,
        rbukrs,
        rldnr,
        paph4_pa,
        paph5_pa,
        vkgrp_pa,
        matnr,
        matnr_copa,
        budat,
        vtweg,
        fiscyearper,
        rcntr,
        hsl,
        quant1,
        osl,
        awtyp,
        kunnr,
        kalnr,
        qsprocess,
        werks,
        case when matnr = '' then matnr_copa else matnr end as _material,

        case
            when
                paph4_pa = ''
                and
                    left(_material, 11) in (
                        '00000000002',
                        '00000000003',
                        '00000000004',
                        '00000000005',
                        '00000000009'
                    )
                and rbukrs != '3100'
            then '98'

            when
                paph4_pa = ''
                and _material in (
                    '000000000020000092',
                    '000000000020000084',
                    '000000000020000087'
                )
                and rbukrs = '3100'
            then '01'
            
            when
                paph4_pa = ''
                and (left(_material, 11) in (
                    '00000000002',
                    '00000000003',
                    '00000000004',
                    '00000000005',
                    '00000000009'
                ))
                and rbukrs = '3100'
                and _material not in (
                    '000000000020000092',
                    '000000000020000084',
                    '000000000020000087'
                )
            then '98'
            
            when
                paph4_pa = ''
                and racct in ('0051121201', '0051183101', '0064180015')
            then '99'

            when racct = '0063242002' and rbukrs = '3100'
            then '01'

            when racct = '0063242001' and rbukrs = '3100'
            then '98'

            else cat_code
        end as _cat_code,

        case
            when
                paph5_pa = ''
                and (
                    left(_material, 11) in (
                        '00000000002',
                        '00000000003',
                        '00000000004',
                        '00000000005',
                        '00000000009'
                    )
                )
                and rbukrs != '3100'
            then '98'
                when
                paph5_pa = ''
                and _material in (
                    '000000000020000092',
                    '000000000020000084',
                    '000000000020000087'
                )
                and rbukrs = '3100'
            then '01'

            when
                paph5_pa = ''
                and (left(_material, 11) in (
                    '00000000002',
                    '00000000003',
                    '00000000004',
                    '00000000005',
                    '00000000009'
                ))
                and rbukrs = '3100'
                and _material not in (
                    '000000000020000092',
                    '000000000020000084',
                    '000000000020000087'
                )
            then '98'
            when
                paph5_pa = ''
                and racct in ('0051121201', '0051183101', '0064180015')
            then '99'

                when
                paph5_pa = ''
                and _material in (
                    '000000000020000092',
                    '000000000020000084',
                    '000000000020000087'
                )
                and rbukrs = '3100'
            then '01'

            when
                paph5_pa = ''
                and (left(_material, 11) in (
                    '00000000002',
                    '00000000003',
                    '00000000004',
                    '00000000005',
                    '00000000009'
                ))
                and rbukrs = '3100'
                and _material not in (
                    '000000000020000092',
                    '000000000020000084',
                    '000000000020000087'
                )
            then '98'

            else subcat1_code
        end as _subcat_1_code,
        case
            when
                paph5_pa = ''
                and (left(_material, 11) in (
                    '00000000002',
                    '00000000003',
                    '00000000004',
                    '00000000005',
                    '00000000009'
                ))
                and rbukrs != '3100'
            then '98'
            when
                paph5_pa = ''
                and racct in ('0051121201', '0051183101', '0064180015')
            then '99'

            when
                paph5_pa = ''
                and _material in (
                    '000000000020000092',
                    '000000000020000084',
                    '000000000020000087'
                )
                and rbukrs = '3100'
            then '01'

            when
                paph5_pa = ''
                and (left(_material, 11) in (
                    '00000000002',
                    '00000000003',
                    '00000000004',
                    '00000000005',
                    '00000000009'
                ))
                and rbukrs = '3100'
                and _material not in (
                    '000000000020000092',
                    '000000000020000084',
                    '000000000020000087'
                )
            then '98'

            else subcat2_code
        end as _subcat_2_code

    from mapping_cat_subcat
),

concatenate_cat_subcat as (

    select
        case 
        when _cat_code is null 
        then '99' 
        else _cat_code end as cat_code_n,

        case
            when cat_code_n = '99'
            then 'DT khác'
            when cat_code_n = '98'
            then 'Khác'
            else cat_name
        end as cat_name_n,

        case
            when _subcat_1_code is null 
            then '99' else _subcat_1_code
        end as subcat_1_code,

        case
            when subcat_1_code = '98'
            then 'Khác'
            when subcat_1_code = '99'
            then 'DT khác'
            else subcat1_name
        end as subcat_1_name,

        case
            when _subcat_2_code is null 
            then '99' else _subcat_2_code
        end as subcat_2_code,

        case
            when subcat_2_code = '98'
            then 'Khác'
            when subcat_2_code = '99'
            then 'DT khác'
            else subcat2_name
        end as subcat_2_name,

        racct as "G/L Account",
        rbukrs as company_code,
        budat as posting_date,
        case when vtweg = '' then '98' else vtweg end as channel_code,
        matnr as matnr,
        fiscyearper,
        rcntr as costcenter_code,
        awtyp,
        kunnr,
        kalnr,
        qsprocess,
        werks,
        sum(
            case
                when
                    (racct >= '0051100000' and racct <= '0051199999')
                    and (racct not in ('0051511000', '0051521000'))
                    or racct = '0064180015'
                then hsl
                else 0
            end

        ) as _revenue,

        sum(
            case
                when
                    (racct >= '0051100000' and racct <= '0051199999')
                    and (racct not in ('0051511000', '0051521000'))
                then quant1
                else 0
            end

        ) as _quantity,

        case
            when _quantity != 0 then (_revenue / _quantity) else 0
        end as "_revenue(D/KG)",

        sum(
            case
                when (racct >= '0063240000' and racct <= '0063289999')
                then hsl
                else 0
            end
        ) as _cogs,

        sum(
            case
                when (racct in ('0063241001','0063241002','0063241003','0063241004','0063242001'
                                ,'0063242002','0063242003','0063242004','0063242005','0063249100',
                                '0063270006','0063270019','0063281001','0063281003','0063283001','0063288004','0063288005') and rbukrs = '3000')
                    or (racct in ('0063241004','0063242001','0063242002','0063281003','0063258100','0063270014','0063270006','0063270019','0063283001') and rbukrs = '3100')
                    or (racct = '0063241001' and rbukrs = '3100' and matnr in ('GAP0490000','GCP0520000'))
                then hsl
                else 0
            end
        ) as material_cost,

        sum(case when racct = '0063241004' then hsl else 0 end) as "_cogs(+)",

        sum(
            case
                when
                    {# (racct >= '0062200000' and racct <= '0062299999') #}
                    (racct in ('0063242006','0063270001','0063270002','0063270004','0063270007','0063270008','0063270009','0063270010','0063270014','0063270018') and rbukrs = '3000') 
                    or (racct in ('0063270004','0063270010','0063270007','0063270002','0063242006','0063270008','0063270009','0063270001') and rbukrs = '3100')
                    then hsl
                    else 0
                    end
        ) as "_manufacturing cost (4)",

        sum(
            case when kpis.gl_manpower_cost is not null then hsl
                else 0
            end
        ) as manpower_cost,

        sum(  
            case
                when
                    (racct >= '0064100000' and racct <= '0064199999')
                    and racct != '0064180015'
                then hsl
                else 0
            end
        ) as "_mkt & commercial costs (6)",

        sum(
            case
                when racct >= '0064200000' and racct <= '0064299999' then hsl else 0
            end
        ) as "_administrative costs (7)",

        sum(
            case
                when racct >= '0063500000' and racct <= '0063599999' then hsl else 0
            end
        ) as "_chi phí tài chính(8)",

        sum(
            case
                when racct >= '0051500000' and racct <= '0051599999' then hsl else 0
            end
        ) as "_thu nhập tài chính(9)"

    from mapping_cat_subcat_special_case
    left join {{ ref("food_int_mapping_kpis__normalized") }} kpis on kpis.gl_manpower_cost = mapping_cat_subcat_special_case.racct
    group by
        cat_code_n,
        cat_name_n,
        _subcat_1_code,
        subcat_1_name,
        _subcat_2_code,
        subcat_2_name,
        "G/L Account",
        company_code,
        posting_date,
        channel_code,
        matnr,
        fiscyearper,
        costcenter_code,
        awtyp,
        kunnr,
        kalnr,
        qsprocess,
        werks
),

final as (

    select
        cat_code_n as cat_code,
        cat_name_n as cat_name,
        subcat_1_code,
        subcat_1_name,
        subcat_2_code,
        subcat_2_name,
        "G/L Account",
        company_code,
        posting_date,
        channel_code,
        matnr,
        fiscyearper,
        costcenter_code,
        kunnr as customer_code,
        kalnr,
        qsprocess,
        werks,
        sum(_revenue * (-1)) as revenue,
        sum(_quantity * (-1)) as quantity,
        sum("_revenue(D/KG)") as "revenue(D/KG)",
        sum(_cogs) as cogs,
        sum(material_cost) as material_cost,
        sum("_cogs(+)") as "cogs(+)",

        abs(revenue) - abs("cogs(+)") as "gross contribution (3) =  (1) - (+)",

        sum("_manufacturing cost (4)") as "manufacturing cost (4)",
        sum(manpower_cost) as manpower_cost,

        abs("gross contribution (3) =  (1) - (+)")
        - abs("manufacturing cost (4)") as "gross margin (5) = (3) - (4)",
        
        sum("_mkt & commercial costs (6)") as "mkt & commercial costs (6)",
        sum("_administrative costs (7)") as "administrative costs (7)",
        sum("_chi phí tài chính(8)") as "chi phí tài chính(8)",
        sum("_thu nhập tài chính(9)") as "thu nhập tài chính(9)"
    from concatenate_cat_subcat
    group by
        cat_code,
        cat_name,
        subcat_1_code,
        subcat_1_name,
        subcat_2_code,
        subcat_2_name,
        "G/L Account",
        company_code,
        posting_date,
        channel_code,
        matnr,
        fiscyearper,
        costcenter_code,
        customer_code,
        kalnr,
        qsprocess,
        werks
        
),

_inter_profit as (

    select 
        kalnr_mat,
        process,
        element,
        sum(prcdif_var) as _prcdif_var,
        sum(prcdif_fix) as _prcdif_fix

    from {{ ref('food_stg_sap_s4__tbl_fcml_ccs_r_all_v') }}
    where curtp = '10' and categ = 'VN' and elesmhk = 'M0'
    group by kalnr_mat, process, element

),

distribution_inter_profit as (
    -- tại sao chỉ 2023005? và tại sao lại distinct
    select distinct
        fi.ktopl,
        fi.kokrs,
        fi.rbukrs,
        fi.rldnr,
        case when fi.matnr = '' then fi.matnr_copa else fi.matnr end as _material,
        fi.fiscyearper,
        fi.kalnr,
        fi.qsprocess,
        fi.werks,
        ip._prcdif_var,
        ip._prcdif_fix,
        ip.element
    from _0fi_acdoca_10_filter as fi
    left join _inter_profit as ip
        on fi.kalnr = ip.kalnr_mat
        and fi.qsprocess = ip.process
        and fi.mlcateg = 'VN'
        and fi.fiscyearper = '2023005'

),

distinct_final as (
    select 
        distinct
        max(cat_code) as _cat_code,
        max(cat_name) as _cat_name,
        max(subcat_1_code) as _subcat_1_code,
        max(subcat_1_name) as _subcat_1_name,
        max(subcat_2_code) as _subcat_2_code,
        max(subcat_2_name) as _subcat_2_name,
        max("G/L Account") as "_G/L Account",
        company_code,
        max(posting_date) as _posting_date,
        max(channel_code) as _channel_code,
        matnr,
        fiscyearper,
        max(costcenter_code) as _costcenter_code,
        kalnr,
        qsprocess,
        werks,
        max(customer_code) as _customer_code
    from final
    group by 
        matnr,
        fiscyearper,
        kalnr,
        qsprocess,
        company_code,
        werks
),

inter_profit_1 as (

    select 
        rbukrs,
        werks,
        _material,
        sum(case
        when ip.element = '010'
        then ip._prcdif_var
        end ) as "10-Raw Material",

        sum(case
        when ip.element = '020'
        then ip._prcdif_var
        end ) as "20-Packaging",

        sum(case
        when ip.element = '030'
        then ip._prcdif_var
        end ) as "30-Subcontract",

        sum(case
        when ip.element = '040'
        then ip._prcdif_var
        end ) as "40-Delivery cost STO",

        sum(case
        when ip.element = '051'
        then ip._prcdif_var
        end ) as "51-Emp.Outsourcing",

        sum(case
        when ip.element = '052'
        then ip._prcdif_var
        end ) as "52-Consum&supplies",

        sum(case
        when ip.element = '053'
        then ip._prcdif_var
        end ) as "53-Energy& utilities",

        sum(case
        when ip.element = '059'
        then ip._prcdif_var
        end ) as "59-Other expenses",

        sum(case
        when ip.element = '061'
        then ip._prcdif_var
        end ) as "61-Wages",

        sum(case
        when ip.element = '062'
        then ip._prcdif_var
        end ) as "62-Other wages",

        sum(case
        when ip.element = '063'
        then ip._prcdif_var
        end ) as "63-Depreciation exp",

        sum(case
        when ip.element = '069'
        then ip._prcdif_var
        end ) as "69-Other expenses",

        sum(case
        when ip.element = '080'
        then ip._prcdif_var
        end ) as "80-merchendise",

        sum(case
        when ip.element = '090'
        then ip._prcdif_var
        end ) as "90-Inter.Profit"
    from distribution_inter_profit as ip
    group by rbukrs, werks, _material

),

inter_profit_2 as (

    select 
        rbukrs,
        werks,
        _material,
        sum(case
        when ip.element = '010'
        then ip._prcdif_fix
        end ) as "10-Raw Material",

        sum(case
        when ip.element = '020'
        then ip._prcdif_fix
        end ) as "20-Packaging",

        sum(case
        when ip.element = '030'
        then ip._prcdif_fix
        end ) as "30-Subcontract",

        sum(case
        when ip.element = '040'
        then ip._prcdif_fix
        end ) as "40-Delivery cost STO",

        sum(case
        when ip.element = '051'
        then ip._prcdif_fix
        end ) as "51-Emp.Outsourcing",

        sum(case
        when ip.element = '052'
        then ip._prcdif_fix
        end ) as "52-Consum&supplies",

        sum(case
        when ip.element = '053'
        then ip._prcdif_fix
        end ) as "53-Energy& utilities",

        sum(case
        when ip.element = '059'
        then ip._prcdif_fix
        end ) as "59-Other expenses",

        sum(case
        when ip.element = '061'
        then ip._prcdif_fix
        end ) as "61-Wages",

        sum(case
        when ip.element = '062'
        then ip._prcdif_fix
        end ) as "62-Other wages",

        sum(case
        when ip.element = '063'
        then ip._prcdif_fix
        end ) as "63-Depreciation exp",

        sum(case
        when ip.element = '069'
        then ip._prcdif_fix
        end ) as "69-Other expenses",

        0 as "80-merchendise",
        0 as "90-Inter.Profit"
    from distribution_inter_profit as ip
    group by rbukrs, werks, _material

),

final_all_inter_profit as (

    select * from inter_profit_1
    union all
    select * from inter_profit_2

),

final_distribution_inter_profit as (

    select
        a._cat_code,
        a._cat_name,
        a._subcat_1_code,
        a._subcat_1_name,
        a._subcat_2_code,
        a._subcat_2_name,
        a."_G/L Account",
        a.company_code,
        a._posting_date,
        a._channel_code,
        a.matnr,
        a.fiscyearper,
        a._costcenter_code,
        a._customer_code,
        a.kalnr,
        a.qsprocess,
        a.werks,
        b1."10-raw material",
        b1."20-packaging",
        b1."30-subcontract",
        b1."40-delivery cost sto",
        b1."51-emp.outsourcing",
        b1."52-consum&supplies",
        b1."53-energy& utilities",
        b1."59-other expenses",
        b1."61-wages",
        b1."62-other wages",
        b1."63-depreciation exp",
        b1."69-other expenses",
        b1."80-merchendise",
        b1."90-inter.profit"
    from distinct_final as a
    inner join final_all_inter_profit as b1
        on a.matnr = b1._material
        and a.company_code = b1.rbukrs
        and a.werks = b1.werks

),

final_all as (

    select 
        cat_code,
        cat_name,
        subcat_1_code,
        subcat_1_name,
        subcat_2_code,
        subcat_2_name,
        "G/L Account",
        company_code,
        posting_date,
        channel_code,
        matnr,
        fiscyearper,
        costcenter_code,
        customer_code,
        '' as kalnr,
        '' as qsprocess,
        werks as plant,
        revenue,
        quantity,
        "revenue(D/KG)",
        cogs,
        material_cost,
        "cogs(+)",
        "gross contribution (3) =  (1) - (+)",
        "manufacturing cost (4)",
        manpower_cost,
        "gross margin (5) = (3) - (4)",
        "mkt & commercial costs (6)",
        "administrative costs (7)",
        "chi phí tài chính(8)",
        "thu nhập tài chính(9)",
        0 as "10-raw material",
        0 as "20-packaging",
        0 as "30-subcontract",
        0 as "40-delivery cost sto",
        0 as "51-emp.outsourcing",
        0 as "52-consum&supplies",
        0 as "53-energy& utilities",
        0 as "59-other expenses",
        0 as "61-wages",
        0 as "62-other wages",
        0 as "63-depreciation exp",
        0 as "69-other expenses",
        0 as "80-merchendise",
        0 as "90-inter.profit"

    from final

    union all

    select
        _cat_code as cat_code,
        _cat_name as cat_name,
        _subcat_1_code as subcat_1_code,
        _subcat_1_name as subcat_1_name,
        _subcat_2_code as subcat_2_code,
        _subcat_2_name as subcat_2_name,
        "_G/L Account" as "G/L Account" ,
        company_code,
        _posting_date as posting_date,
        _channel_code as channel_code,
        matnr,
        fiscyearper,
        _costcenter_code as costcenter_code,
        _customer_code as customer_code,
        kalnr,
        qsprocess,
        werks as plant,
        0 as  revenue,
        0 as  quantity,
        0 as  "revenue(D/KG)",
        0 as  cogs,
        0 as  material_cost,
        0 as  "cogs(+)",
        0 as  "gross contribution (3) = (1) - (+)",
        0 as  "manufacturing cost (4)",
        0 as   manpower_cost,
        0 as  "gross margin (5) = (3) - (4)",
        0 as  "mkt & commercial costs (6)",
        0 as  "administrative costs (7)",
        0 as  "chi phí tài chính(8)",
        0 as  "thu nhập tài chính(9)",
        "10-raw material",
        "20-packaging",
        "30-subcontract",
        "40-delivery cost sto",
        "51-emp.outsourcing",
        "52-consum&supplies",
        "53-energy& utilities",
        "59-other expenses",
        "61-wages",
        "62-other wages",
        "63-depreciation exp",
        "69-other expenses",
        "80-merchendise",
        "90-inter.profit"

    from final_distribution_inter_profit

)

select * from final_all