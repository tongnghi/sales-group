with opening_balance as (

    select 
        plant,
        material,
        sum(qty) as qty,
        sum(primary_material) as primary_material,
        sum(valued) as valued

    from {{ ref("feed_int_zmllistn__calculate_opening_balance") }}
    group by plant, material
    
),


internal as (
    
    select 
        int_entry.plant,
        int_entry.material,
        nvl(sum(int_entry.qty),0) as qty_,
        nvl(sum(inccured_costs.raw_gia_version),0) as raw_gia_version_,
        qty_ * raw_gia_version_ as thanhtien

    from {{ ref("feed_int_ke24__internal_entry") }} int_entry
    left join {{ ref("feed_int_zppr0019__incurred_costs") }} inccured_costs on int_entry.plant = inccured_costs.plant and int_entry.material = inccured_costs.material
    group by int_entry.plant, int_entry.material

),

gia_version as (
    
    select
        *
    from {{ ref("feed_int_zppr0019__incurred_costs") }}

),

giavon_code as (
    
    select
        plant,
        material
    from gia_version

    union

    select 
        plant,
        material
    from {{ ref("feed_stg_sap_ecc__ke24") }}
    where perio = '00082023' and material_group like 'C1%%' and sched_line_cat in ('Z1','Z2','Z4','Z5','Z6') 
),

giavon_draft as (

    select
        version_code.plant,
        version_code.material,
        sum(op_balance.qty) as opening_qty,
        sum(op_balance.valued) as opening_amt,
        nvl(sum(internal.qty_),0) as internal_qty,
        nvl(sum(internal.thanhtien),0) as internal_amt,
        sum(gia_version.sanluongxacnhan_DVcoban_final) as version_qty,
        sum(gia_version.giatrixn_final) as version_amt,
        opening_qty + internal_qty + version_qty as quantity,
        opening_amt + internal_amt + version_amt as amount,
        amount/quantity as gia_donvi

    from giavon_code version_code
    left join
    opening_balance op_balance
    on version_code.plant = op_balance.plant and version_code.material = op_balance.material
    left join
    internal 
    on version_code.plant = internal.plant and version_code.material = internal.material
    left join
    gia_version 
    on version_code.plant = gia_version.plant and version_code.material = gia_version.material
    group by version_code.plant, version_code.material

),

group_giavon_draft as (
    
    select
        plant,
        material,
        sum(quantity) as qty,
        sum(amount) as amt,
        amt/qty as gia_sanpham

    from giavon_draft
    group by plant,material

),

thaybao_final as (
    
    select
        thaybao.plant,
        thaybao.material,
        thaybao.ma_thanh_phan,
        sum(thaybao.sanluongxacnhan_DVcoban) as sl,
        sum(group_giavon_draft.gia_sanpham) as price,
        sl*price as thanhtien

    from {{ ref("feed_int_zppr0019_thaybao") }} thaybao
    left join group_giavon_draft 
    on thaybao.plant = group_giavon_draft.plant and thaybao.ma_thanh_phan = group_giavon_draft.material
    group by thaybao.plant, thaybao.material, thaybao.ma_thanh_phan

),

transfer_code as (

    select
        plant,
        material,
        sum(qty) as qty_

    from {{ ref("feed_stg_sap_ecc__mb51") }}
    where movement_type = '945'
    group by  plant, material

),

transfer_code_final as (

    select
        transfer_code.plant,
        transfer_code.material,
        sum(transfer_code.qty_) as sl,
        sum(group_giavon_draft.gia_sanpham) as price,
        sl*price as thanhtien

    from transfer_code 
    left join group_giavon_draft 
    on transfer_code.plant = group_giavon_draft.plant and transfer_code.material = group_giavon_draft.material
    group by transfer_code.plant, transfer_code.material

),

giavon_final as (

    select
        version_code.plant,
        version_code.material,
        sum(op_balance.qty) as opening_qty,
        sum(op_balance.valued) as opening_amt,
        nvl(sum(internal.qty_),0) as internal_qty,
        nvl(sum(internal.thanhtien),0) as internal_amt,
        nvl(sum(gia_version.sanluongxacnhan_DVcoban_final),0) as version_qty,
        nvl(sum(gia_version.giatrixn_final),0) as version_amt,
        sum(gia_version.baobi) as giabaobi,
        nvl(sum(thaybao_final.sl),0) as thaybao_qty,
        nvl(sum(thaybao_final.thanhtien),0) as thaybao_amt,
        nvl(sum(transcode.sl),0) as transfer_qty,
        nvl(sum(transcode.thanhtien),0) as transfer_amt,
        opening_qty + internal_qty + version_qty + thaybao_qty + transfer_qty  as quantity,
        opening_amt + internal_amt + version_amt + thaybao_amt + transfer_amt  as amount,
        amount/quantity as gia_donvi

    from giavon_code version_code
    left join
    opening_balance op_balance
    on version_code.plant = op_balance.plant and version_code.material = op_balance.material
    left join
    internal 
    on version_code.plant = internal.plant and version_code.material = internal.material
    left join
    gia_version 
    on version_code.plant = gia_version.plant and version_code.material = gia_version.material
    left join thaybao_final 
    on version_code.plant = thaybao_final.plant and version_code.material = thaybao_final.material
    left join transfer_code_final transcode
    on version_code.plant = transcode.plant and version_code.material = transcode.material
    group by version_code.plant, version_code.material

)

    select 
        plant,
        material,
        quantity,
        amount,
        gia_donvi,
        giabaobi
    from giavon_final
    where quantity > 0 