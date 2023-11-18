with raw_bf as (
    -- inventory phát sinh
    select 
        matnr, --san pham
        bwart, -- momentype
        budat, -- posting date
        lgort, -- store log (store location)
        werks, -- plant
        charg, -- batch_number
        bstaus, -- stock_type
        bsttyp, -- stock category
        lifnr, -- vendor
        bukrs, -- company code
        sobkz, -- special stock ind
        mblnr, -- document number
        zeile, -- document item number
        -- To do: Refactor condition
        -- except movement type 343 TR blocked
        case
            when (bwvorg = '000' or bwvorg = '001' or bwvorg = '004'
                or bwvorg = '005' or bwvorg = '006' or bwvorg = '010'
                or bwvorg = '002' or bwvorg = '003' or bwvorg = '007')
                and (bwapplnm = 'MM' or bwapplnm = 'IS-R')
                and bwbrel = '1'
                and (bsttyp is not null or (bsttyp like '%EQ%' and kzbws like '%AM%'))
            then
                case
                    when bwgeo = 0 and bwapplnm = 'IS-R' then
                        case when rocancel = 'X' then dmbtr * (-1) else dmbtr end
                    else bwgeo
                end
            else 0
        end as prevs_val_inflow,

        case
            when (bwvorg = '100'
                or bwvorg = '101'
                or bwvorg = '104'
                or bwvorg = '105'
                or bwvorg = '106'
                or bwvorg = '110'
                or bwvorg = '102'
                or bwvorg = '103'
                or bwvorg = '107')
                and (bwapplnm = 'MM' or bwapplnm = 'IS-R')
                and bwbrel = '1'
                and (bsttyp is not null or (bsttyp like '%EQ%' and kzbws like '%AM%'))
            then
                case
                    when bwmng = 0 and bwapplnm = 'IS-R' then
                        case when rocancel = 'X' then bwgeo else dmbtr end
                    else
                        case when rocancel = 'X' then bwgeo else dmbtr end
                end

            else 0
        end as pisvs_val_outflow,

        case
            when (bwvorg = '000'
                or bwvorg = '001'
                or bwvorg = '004'
                or bwvorg = '005'
                or bwvorg = '006'
                or bwvorg = '010'
                or bwvorg = '002'
                or bwvorg = '003'
                or bwvorg = '007')
                and (bwapplnm = 'MM' or bwapplnm = 'IS-R')
                and bwbrel = '1'
                and (bsttyp is not null or (bsttyp like '%EQ%' and kzbws like '%AM%'))
            then
                case
                    when bwmng = 0 and bwapplnm = 'IS-R' then
                        case when rocancel = 'X' then menge * (-1) else menge end
                    else bwmng
                end
            else 0
        end as pretotstk_inflow,

        case
            when (bwvorg = '100'
                or bwvorg = '101'
                or bwvorg = '104'
                or bwvorg = '105'
                or bwvorg = '106'
                or bwvorg = '110'
                or bwvorg = '102'
                or bwvorg = '103'
                or bwvorg = '107')
                and (bwapplnm = 'MM' or bwapplnm = 'IS-R')
                and bwbrel = '1'
                and (bsttyp is not null or (bsttyp like '%EQ%' and kzbws like '%AM%'))
            then
                case
                    when bwmng = 0 and bwapplnm = 'IS-R' then
                        case when rocancel = 'X' then menge * (-1) else menge end
                    else bwmng
                end
            else 0
        end as pistotstk_outflow

    from {{ ref('food_stg_sap_s4__mm_2lis_03_bf') }}

),

raw_um as (
    -- giá trị inventory điều chỉnh
    select 
        budat, -- posting date
        bukrs,    --company
        matnr,    --material
        bwart,   -- movement type
        werks,    --plant
        lifnr,    --vendor
        belnr,    -- document number
        sobkz,    --special stock ind
        bsttyp,   --stock category
        bstaus,   --stock type
        '' as charg,
        null as zeile,
        '' as lgort,

        case
            when 
                (bwvorg = '050' or bwvorg = '051' or bwvorg = '052')
                and bwapplnm = 'MM'
                and bwgeo != 0 
            then bwgeo
            else 0 
        end as prevs_val_inflow,

        case
            when 
                (bwvorg = '150' or bwvorg = '151' or bwvorg = '152') 
                and bwapplnm = 'MM'
                and bwgeo != 0 
            then bwgeo
            else 0
        end as pisvs_val_outflow,
        
        0 as pretotstk_inflow,
        0 as pistotstk_outflow

    from {{ ref('food_stg_sap_s4__mm_2lis_03_um') }}
    where bsttyp != 'V' or bstaus != 'V'

),

unioned as (
    
    select
        budat,
        matnr,
        bwart,
        lgort,
        werks,
        charg, 
        bstaus,
        bsttyp,
        lifnr,
        bukrs, 
        sobkz, 
        mblnr, 
        zeile,
        prevs_val_inflow,
        pisvs_val_outflow,
        pretotstk_inflow,
        pistotstk_outflow
    from raw_bf
    union all
    select 
        budat, 
        matnr, 
        bwart, 
        lgort, 
        werks,
        charg, 
        bstaus, 
        bsttyp, 
        lifnr,
        bukrs, 
        sobkz, 
        belnr, 
        zeile,
        prevs_val_inflow,
        pisvs_val_outflow,
        pretotstk_inflow,
        pistotstk_outflow
    from raw_um

),

final as (
    
    select 
        budat, 
        matnr, 
        bwart, 
        case when lgort != '' then lgort else sobkz end lgort, 
        werks,
        charg, 
        bstaus,
        bsttyp, 
        lifnr,
        bukrs, 
        sobkz, 
        mblnr, 
        zeile,

        mvt.type,
        sum(case when mvt.type = 'selling' then pistotstk_outflow else 0 end) as issues_stock,
        sum(case when mvt.type = 'manufacturing' then pretotstk_inflow else 0 end) as reciept_stock,
        sum(case when mvt.type = 'selling' then pisvs_val_outflow else 0 end) as issues_value,
        sum(case when mvt.type = 'manufacturing' then prevs_val_inflow else 0 end) as reciept_value,

        sum(case when mvt.in_out = 'import' then prevs_val_inflow else 0 end) as prevs_val_inflow,
        sum(case when mvt.in_out = 'export' then pisvs_val_outflow else 0 end) as pisvs_val_outflow,
        sum(case when mvt.in_out = 'import' then pretotstk_inflow else 0 end) as pretotstk_inflow,
        sum(case when mvt.in_out = 'export' then pistotstk_outflow else 0 end) as pistotstk_outflow,
        sum(prevs_val_inflow) - sum(pisvs_val_outflow) as sub_value,
        sum(pretotstk_inflow) - sum(pistotstk_outflow) as sub_stock

    from unioned
    left join {{ ref('food_seed_inventory_mapping_movement_type') }} mvt on mvt.mvt_code = unioned.bwart
    group by budat, matnr, bwart, lgort, werks, charg, bstaus, bsttyp, lifnr, bukrs, sobkz, mblnr, zeile, mvt.type

)

select * from final