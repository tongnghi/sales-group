with distinct_bill_doc as (
    -- Todo: deuplicated need to review (vbeln, posnr)
    select distinct vbeln, posnr, pkunwe, pkunre, pstyv, vgbel
    from {{ ref("food_stg_sap_s4__sd_2lis_13_vditm") }}

),

product_attr as (

    select * from {{ ref("food_stg_sap_s4__md_0material_attr") }}
       
),

sales_order_item as (

    select 
        vbeln, 
        posnr,
        augru,
        row_number() over (partition by vbeln, posnr order by augru, rocancel ,vdatu desc) as dedup
    from {{ ref("food_stg_sap_s4__sd_2lis_11_vaitm") }}

),

deduped_so as (

    select * from sales_order_item where dedup = 1

),

sale_person as (

    select vbeln, kunnr, parvw from {{ ref('food_stg_sap_s4__tbl_vbpa') }}
    where parvw in ('Z1','Z2','Z3','Z4','Z5','AG','WE','RE')

),

raw_fi as (

    select
        budat,
        fiscyearper,
        rbukrs,
        werks,
        vkbur_pa,
        vkorg,
        vkgrp_pa,
        bzirk,
        vtweg,
        case when matnr = '' then matnr_copa else matnr end as product_code,
        prodh_pa,
        paph1_pa,
        paph2_pa,
        paph3_pa,
        paph4_pa,
        paph5_pa,
        paph6_pa,
        paph7_pa,
        paph8_pa,
        kdgrp,
        kunnr,
        kvgr1_pa,
        kvgr2_pa,
        racct,
        spart,
        belnr,
        fkart,
        case when awtyp = 'VBRK' then awref else '' end as pbillnum,
        case when awtyp = 'VBRK' then awitem else '' end as pbillitem,
        kdauf as psodocno,
        kdpos as psodocln,
        runit,
        sum(msl) as bill_quantity,
        sum(quant1) as bill_quantity_in_kg,
        sum(hsl) as bill_net_amount
        
    from {{ ref("food_stg_sap_s4__fi_0fi_acdoca_10") }}
    where racct between '0051100000' and '0051900000'
        and ktopl = '1000'
        and scope = 'PA'
        and paobjnr != '0000000000'
        and paobjnr != ''
        and rldnr = '0L'
    {{ dbt_utils.group_by(n=32) }}

),


fi_sale_hm as (

    select
        budat as posting_date,
        fiscyearper as period_year,
        rbukrs as company_code,
        werks as plant_code,
        vkbur_pa as sales_office_code,
        vkorg as sales_organization_code,
        vkgrp_pa as sales_group_code,
        bzirk as sales_district_code,
        vtweg as distribution_channel_code,
        product_code,
        prodh_pa as product_hierachy_code,
        paph1_pa as product_hierarchy_01_1_code,
        paph2_pa as product_hierarchy_01_2_code,
        paph3_pa as product_hierarchy_01_3_code,
        paph4_pa as product_hierarchy_01_4_code,
        paph5_pa as product_hierarchy_01_5_code,
        paph6_pa as product_hierarchy_01_6_code,
        paph7_pa as product_hierarchy_01_7_code,
        paph8_pa as product_hierarchy_01_8_code,
        kdgrp as customer_group_code,
        kvgr1_pa as customer_group_1_code,
        kvgr2_pa as customer_group_2_code,
        racct as gl_account_code,
        spart as division_code,
        fkart as billing_type,
        psodocno,
        psodocln,
        pbillnum,
        pbillitem,
        distinct_bill_doc.pstyv as catagory_item,  -- item category
        belnr as document_number,
        runit as unit,
        bill_quantity * (-1) as bill_quantity,
        bill_quantity_in_kg * (-1) as bill_quantity_in_kg,
        bill_net_amount * (-1) as bill_net_amount,
        kunnr as sold_to,  -- sold to party
        deduped_so.augru as reason_code,

        case when runit = 'EA' and product_attr.weight_unit = 'KGM' and product_code = 'GCP0890000' then 14
            else product_attr.net_weight end as net_weight,
            
        case
            when psodocno != ''
            then
                (
                    select kunnr from sale_person
                    where sale_person.vbeln = raw_fi.psodocno
                         and sale_person.parvw = 'RE'
                )
        end as bill_to,  -- bill to party
        case
            when psodocno != ''
            then
                (
                    select kunnr from sale_person
                    where sale_person.vbeln = raw_fi.psodocno
                         and sale_person.parvw = 'WE'
                )
        end as ship_to,  -- ship to party

        case
            when psodocno != ''
            then
                (
                    select kunnr from sale_person
                    where sale_person.vbeln = raw_fi.psodocno
                         and sale_person.parvw = 'Z1'
                )
        end as pempz11,  -- * PG/Salesman

        case
            when psodocno != ''
            then
                (
                      select kunnr from sale_person
                    where sale_person.vbeln = raw_fi.psodocno
                         and sale_person.parvw = 'Z2'
                )
        end as pempz12,  -- * Sales executive

        case
            when psodocno != ''
            then
                (
                    select kunnr from sale_person
                    where sale_person.vbeln = raw_fi.psodocno
                    and sale_person.parvw = 'Z3'
                )
        end as pempz13,  -- * Sales supervisor

        case
            when psodocno != ''
            then
                (
                    select kunnr from sale_person
                    where sale_person.vbeln = raw_fi.psodocno
                    and sale_person.parvw = 'Z4'
                )
        end as pempz14,  -- * Sales ASM

        case
            when psodocno != ''
            then
                (
                    select kunnr from sale_person
                    where sale_person.vbeln = raw_fi.psodocno
                    and sale_person.parvw = 'Z5'
                )
        end as pempz15  -- * Sales RSM

    from raw_fi
    left join distinct_bill_doc on raw_fi.pbillnum = distinct_bill_doc.vbeln 
                        and raw_fi.pbillitem = distinct_bill_doc.posnr
    left join deduped_so on raw_fi.psodocno = deduped_so.vbeln
                        and raw_fi.psodocln = deduped_so.posnr
    left join product_attr on raw_fi.product_code = product_attr.code

)
    
select * from fi_sale_hm
