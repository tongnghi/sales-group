with renamed_cast as (

    select
        '00082023' as perio,
        "sales organization"::varchar as sales_org,
        sales_org as plant,
        to_date("posting date",'DD-MM-YYYY',false) as posting_date,
        "sales group"::varchar as sales_group,
        "sales office"::varchar as sales_office,
        "sales manager"::varchar as sales_manager,
        "sched.line category" as sched_line_cat,
        "distribution channel"::varchar as dis_channel,
        division::varchar,
        "product" as material,
        "material group" as material_group,
        {# "material type" as material_type, #}
        {# "customer group"::varchar as customer_group, #}
        sum("billed qty (sku)") as billed_qty,
        sum(replace("1.giá xuất xưởng",',','')::numeric) as factory_price,
        sum("1.giá đc tăng(drmem)") as increasing_price ,
        sum("1.giá đc giảm(crmem)") as decreasing_price ,
        sum("1.ck giá %") as discount_price ,
        sum("1.ck giá đ/kg") as discount_price_d_kg ,
        sum("1.phí chuyển hàng") as delivery_fee,
        sum(replace("1.giá hóa đơn",',','')::numeric) as gia_hoa_don,
        sum("1.giá trả hàng") as refund_price,
        sum("1.cktt sau hĐ (%)") as CKTT_sauHD,
        sum("1.cktt sau hĐ (đ/kg)") as CKTT_sauHD_d_kg,
        sum("1.t.toán ck") as thanhtoan_CK,
        sum("1.discoun by % ratio") as discount_by_ratio,
        sum("1.discount by d/kg") as discount_by_d_kg,
        sum(replace("standard cogs",',','')::numeric) as standard_cogs,
        sum(replace("std.pri material cst",',','')::numeric) as primaterial_cost,
        sum(replace("std.packaging cost",',','')::numeric) as packaging_cost,
        sum("std.labor cost") as labor_cost,
        sum("std.machinery cost") as machinery_cost,
        sum("std.electricity cost") as electricity_cost,
        sum("std.steam cost") as steam_cost,
        sum("std.equipment cost") as equipment_cost,
        sum("std.overhead") as overhead,
        sum("std.subcontract") as subcontract

from feed.nghia_dev.feed_seed_margin_draft_ke24_008_
group by perio,plant,sales_org,posting_date,sales_group,sales_office,sales_manager,sched_line_cat,dis_channel,division,material,material_group



)
select * from renamed_cast

