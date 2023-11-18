{{ config(materialized="table") }}
select
    s.companycode as compcode,
    s.billingtype,
    s.source as db,
    '' as so,
    '' as do,
    s.trandatemonthid::character(500) as perpost,
    s.trandate,
    s.batnbr,
    '' as trantype,
    '' as account,
    s.sub_fix as sub,
    '' as invoiceno,
    '' as lineref,
    0 as attackqty,
    s.salesmanager as slsperid,
    sm.salesmanagername as salesperson,
    s.customer as custid,
    s.customername as name,
    case
        when account in ('13682000', '13684000')
        then 'Internal'
        when
            s.customer in (
                'F800000',
                'N23B000000',
                'N26B000000',
                'N26C000000',
                'N39B000000',
                'N45B000000',
                'N58B000000',
                'S02G000058',
                'S02G000060',
                'S20C000000',
                'S02G000059',
                'S02G000592',
                'N04G000822',
                'N06G000483',
                'S02G000056',
                'S02G000058',
                '101407',
                '104984',
                '105133',
                '107331',
                '108467',
                '108873'
            )
        then 'Internal'
        when
            (
                s.customer = 'S07G000894'
                and right(trim(s.sub_fix), 3) not in ('D71', 'C01')
            )
        then 'Internal'
        else 'Customer'
    end as channel,
    m.materialname as descr,
    '03B' as farm_code,
    right(left(s.sub_fix, 8), 3) as pvkd,
    right(s.sub_fix, 3) as product_code,
    'Pig' as type_product,
    'Sales' as issue_purpose,
    0 as qty_so,
    0 as qty,
    0 as qty_in,
    s."giá x.xưởng act" as tranamt,
    0 as curytransportfee,
    0 as discountamt,
    0 as volume,
    0 as weight,
    s."san lg act" as volume_new,
    0 as weight_new,
    '107' as farmid,
    'Cam My' as farmname,
    'HEO' as ftype,
    '1020' as buid,
    '10' as regionid
from
    (
        select
            case
                when material in ('S521020V3010')
                then 'B103BS00B31'
                when material in ('S521040V1010', 'S521040V3010', 'S521040V7010')
                then 'B103BS00B32'
                when
                    material in (
                        'S510280V3010',
                        'S510280V7010',
                        'S510337V1010',
                        'S510337V3010',
                        'S510337V7010',
                        'S510399V1010',
                        'S510399V3010',
                        'S510399V7010'
                    )
                then 'B103BS00B33'
                else ''
            end as sub_fix,
            *
        from {{ ref("farm_stg_redshift__sales_cammy") }}
    ) s
left join
    {{ source("farm_redshift__prd__dwh", "d_material") }} m
    on s.materialid = m.materialid
left join
    {{ source("farm_redshift__prd__dwh", "d_distributionchannel") }} ch
    on s.distributionchannelid = ch.distributionchannelid
left join
    {{ source("farm_redshift__prd__dwh", "d_salesmanager") }} sm
    on s.salesmanagerid = sm.salesmanagerid
