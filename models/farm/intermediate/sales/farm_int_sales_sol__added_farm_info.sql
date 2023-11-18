{{ config(materialized="table") }}

select
    case
        when f.compcode is not null then f.compcode else u.company_code
    end as company_code,
    billing_type,
    db as "database",
    so as sales_order,
    "do" as delivery_order,
    perpost,
    trandate,
    batnbr as batch_number,
    trantype as invoice_type,
    account as gl_account,
    u.sub,
    invoiceno as invoice_number,
    lineref,
    attackqty,
    slsperid as sales_person_id,
    salesperson as sales_person_name,
    custid as customer_id,
    name as customer_name,
    case
        when
            account in ('13682000', '13684000')
            or custid in (
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
            or (custid = 'S07G000894' and right(trim(u.sub), 3) not in ('D71', 'C01'))
        then 'Internal'
        else 'Customer'
    end as channel,
    descr as description,
    farm_code,
    right(left(u.sub, 8), 3) as pvkd,  -- EN là gì?
    right(trim(u.sub), 3) as product_code,
    case
        when left(right(trim(u.sub), 3), 1) = 'P' then 'Chicken' else 'Pig'
    end as product_type,
    case
        when left(account, 5) = '13682' then 'Internal Use' else 'Sales'
    end as issue_purpose,
    case
        when ftype = 'GA' or ftype = 'GAG' and trantype = 'CM'
        then qty_so * (-1)
        else qty_so
    end as qty_so,
    qty,
    qty_in,
    case
        when billing_type = 'ZS1'
        then
            (
                case
                    when
                        right(left(u.sub, 5), 3) in ('18C', '45B')
                        and left(account, 5) = '13684'
                    then 0
                    else tranamt
                end
            )
            * (-1)
        else
            (
                case
                    when
                        right(left(u.sub, 5), 3) in ('18C', '45B')
                        and left(account, 5) = '13684'
                    then 0
                    else tranamt
                end
            )
    end as amount,  -- tranamt
    case
        when billing_type = 'ZS1' then curytransportfee * (-1) else curytransportfee
    end as transport_fee,  -- curytransportfee,
    case
        when billing_type = 'ZS1' then discountamt * (-1) else discountamt
    end as discount_amount,  -- discountamt,
    case
        when issue_purpose = 'Internal Use'
        then qty
        when product_code in ('P50', 'P60', 'P62', 'P51', 'P52')
        then qty
        when left(product_code, 1) != 'P'
        then qty_so
        else qty_so
    end as volume,
    case
        when issue_purpose = 'Internal Use'
        then 100 * qty
        when
            product_code
            in ('P50', 'P60', 'B31', 'B32', 'B33', 'P51', 'P52', 'P62', 'P15', 'P16')
        then 0
        else qty
    end as "weight",
    case
        when billing_type = 'ZS1'
        then
            (
                case
                    when volume = 0
                    then 0
                    when qty < 0 and volume > 0
                    then -1 * volume
                    else volume
                end
            )
        else
            (
                case
                    when volume = 0
                    then 0
                    when qty < 0 and volume > 0
                    then -1 * volume
                    else volume
                end
            )
    end as volume_new,
    (
        case
            when product_type = 'Pig' and product_code not in ('D75', 'D80')
            then "weight" + attackqty
            else "weight"
        end
    )
    * (case when billing_type = 'ZS1' then -1 else 1 end) as weight_new,
    farmid as farm_id,
    farmname as farm_name,
    ftype as farm_type,
    buid as bu_id,
    regionid as region_id
from
    (
        select right(left(sub, 5), 3) as farm_code, *
        from {{ ref("farm_int_sales_sol__unioned_adj_rev") }}
    ) u

left join {{ ref("farm_int_farms__unioned") }} f on u.farm_code = f.sub
