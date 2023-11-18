with doanh_thu_draft as (

    select 
        farm_sales.*,
        case when unitdesc in ('Con','KG') then unitdesc
            else 'KM' end as unit,
        right(sub,3) as seg4,
        substring(sub,3,3) as seg2,
        left(right(sub,3),1) as seg4_2,
        nvl(food_customerid.note,'0') as food
        -- price_variance,
        --  Value_Variance
        
    from {{ ref("farm_stg_excel__farm_sales") }} farm_sales
    left join {{ ref("farm_stg_excel__customer") }} food_customerid on farm_sales.custid = food_customerid.custid
),
-- TODO: tại sao lại tách ra với cái trên -> do phải có cột seg4 được tạo ra để join với bảng mới
doanh_thu as (

    select 
        doanh_thu_draft.db,
        doanh_thu_draft._do,
        doanh_thu_draft.perpost,
        doanh_thu_draft.batnbr,
        doanh_thu_draft._status,
        doanh_thu_draft.trantype,
        doanh_thu_draft.account,
        doanh_thu_draft.sub,
        doanh_thu_draft.trandate,
        doanh_thu_draft.descr,
        doanh_thu_draft.unitdesc,

        case when doanh_thu_draft.unitdesc = 'GGCS' then 0 else doanh_thu_draft.qty_so end as qty_so,

        case when doanh_thu_draft.unitdesc = 'GGCS' then 0 else doanh_thu_draft.qty_in end as qty_in,

        doanh_thu_draft.unitprice,

        case when doanh_thu_draft.sub in ('D75','D80') then 0
            else doanh_thu_draft.attackqty end as attackqty,

        doanh_thu_draft.curyattackprice,
        doanh_thu_draft.km,
        doanh_thu_draft.dgvc,
        doanh_thu_draft.curytransportfee,
        doanh_thu_draft.tiendhang,
        doanh_thu_draft.tienphigiong_trongluongvuot,
        doanh_thu_draft.tongthanhtien,  
        doanh_thu_draft.name,
        doanh_thu_draft.custid,
        doanh_thu_draft.invoiceno,
        doanh_thu_draft.sophieuxuat,
        doanh_thu_draft.slsperid,
        doanh_thu_draft.code_reporting,
        doanh_thu_draft.projectid,
        doanh_thu_draft.tongtrongluong,
        doanh_thu_draft.region,
        doanh_thu_draft.unit,

        case when doanh_thu_draft.account in (' 52114002', '52114102') and doanh_thu_draft.sub in ('D75','D76','D77','D78','D79') then 0
            when (case when doanh_thu_draft.seg4 like 'B%%' then 'Semen'
                        when doanh_thu_draft.seg4 like 'P%%' then 'Poultry'
                        else 'Swine' end) = 'Swine' then doanh_thu_draft.tongthanhtien - doanh_thu_draft.curytransportfee
            else doanh_thu_draft.tongthanhtien end as total_amt,

        --check lại xem doanh thu bán heo có gồm tinh heo không???
        doanh_thu_draft.seg4,
        nvl(farm_pg.type,'0') as pg,
        doanh_thu_draft.seg2,
        doanh_thu_draft.food,
        doanh_thu_draft.trandate as _date,
        doanh_thu_draft.descr as _description,
        doanh_thu_draft.sub as subaccount,
        doanh_thu_draft.qty_so as quantity,
        qty_in + attackqty as weight,
        doanh_thu_draft.unitprice as interal_price,
        total_amt as amount_internal,
        doanh_thu_draft.unitprice,
        total_amt as amount_external,

        case when farm_pg.sub isnull then seg4
            else farm_pg.sub end as sub_report,

        case when doanh_thu_draft.seg4 like 'B%%' then 'Semen'
            when doanh_thu_draft.seg4 like 'P%%' then 'Poultry'
            else 'Swine' end as industry,

        case when doanh_thu_draft.db = 'FASIAAPP' then 'ASIA'
            when doanh_thu_draft.db = 'MTR2002APP' then 'DLK'
            when doanh_thu_draft.db = 'MTR2001APP' then 'LFBT'
            when doanh_thu_draft.db = 'MTR1001APP' then 'LFBD'
            when doanh_thu_draft.db = 'DNB2001APP' then 'DNB1'
            when doanh_thu_draft.db = 'ARWHYAPP' then 'HY'
        end as entity_code,

        case when entity_code = 'HY' and doanh_thu_draft.seg2 = '45B' and doanh_thu_draft.seg4_2 = 'D' then 'Hung Yen'
            when entity_code = 'HY' and doanh_thu_draft.seg2 = '45B' and doanh_thu_draft.seg4_2 = 'B' then 'Hung Yen Semen'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '58B' and doanh_thu_draft.seg4_2 = 'D' then 'Bac Can 1'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '58B' and doanh_thu_draft.seg4_2 = 'B' then 'Bac Can 1 Semen'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '59B' and doanh_thu_draft.seg4_2 = 'D' then 'Yen Bai'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '58C' and doanh_thu_draft.seg4_2 = 'D' then 'Bac Can 2'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '39B' and doanh_thu_draft.seg4_2 = 'D' then 'Hoa Binh'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '48B' and doanh_thu_draft.seg4_2 = 'D' then 'Thanh Hoa 2'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '48C' and doanh_thu_draft.seg4_2 = 'D' then 'Thanh Hoa 1'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '48D' and doanh_thu_draft.seg4_2 = 'D' then 'Thanh Hoa 3'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '26B' and doanh_thu_draft.seg4_2 = 'D' then 'Gia Lai 1'
            when entity_code = 'DLK' and doanh_thu_draft.seg2 = '23B' and doanh_thu_draft.seg4_2 = 'D' then 'Dak Lak 1'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '26C' and doanh_thu_draft.seg4_2 = 'D' then 'Gia Lai 2'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '26D' and doanh_thu_draft.seg4_2 = 'D' then 'Gia Lai 5'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '26E' and doanh_thu_draft.seg4_2 = 'D' then 'Gia Lai 6'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '31B' and doanh_thu_draft.seg4_2 = 'D' then 'Quang Nam 1'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '27B' and doanh_thu_draft.seg4_2 = 'D' then 'Phu Yen'
            when entity_code = 'ASIA' and doanh_thu_draft.seg2 = '17B' and doanh_thu_draft.seg4_2 = 'D' then 'Binh Thuan'
            when entity_code = 'ASIA' and doanh_thu_draft.seg2 = '19B' and doanh_thu_draft.seg4_2 = 'D' then 'Dong Nam Bo 2'
            when entity_code = 'DNB1' and doanh_thu_draft.seg2 = '18C' and doanh_thu_draft.seg4_2 = 'D' then 'Dong Nam Bo 1'
            when entity_code = 'LFBT' and doanh_thu_draft.seg2 = '88B' and doanh_thu_draft.seg4_2 = 'D' then 'Lang Viet Nam'
            when entity_code = 'LFBT' and doanh_thu_draft.seg2 = '85B' and doanh_thu_draft.seg4_2 = 'D' then 'Lang Viet 1'
            when entity_code = 'LFBT' and doanh_thu_draft.seg2 = '86B' and doanh_thu_draft.seg4_2 = 'D' then 'Lang Viet 2'
            when entity_code = 'ASIA' and doanh_thu_draft.seg2 = '60B' and doanh_thu_draft.seg4_2 = 'D' then 'Cujut'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '20B' and doanh_thu_draft.seg4_2 = 'D' then 'Tay Ninh 1'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '20C' and doanh_thu_draft.seg4_2 = 'D' then 'Tay Ninh 2'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '16B' and doanh_thu_draft.seg4_2 = 'D' then 'Ca Mau 1'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '20D' and doanh_thu_draft.seg4_2 = 'D' then 'Tay Ninh 3'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '20E' and doanh_thu_draft.seg4_2 = 'D' then 'Tay Ninh 4'
            when entity_code = 'LFBD' and doanh_thu_draft.seg2 = '60C' and doanh_thu_draft.seg4_2 = 'D' then 'Dak Nong 2'
            when entity_code = 'LFBD' and doanh_thu_draft.sub in ('D246R000P00','D269F021P00','D246RM21P00','D239C000P00','D244C000P00','D244D000P00','D244E000P00','D244G000P00','D244H000P00','D244J000P00','D244K000P00','D244M000P00','D244N000P00','D244O000P00','D244P000P00','D244Q000P00','D244S000P00','D246D000P00','D246S000P00','D247D000P00','D251D000P00','D252D000P00','D252E000P00','D252F000P00','D252T000P00','D252V000P00','D252W000P00','D252X000P00','D253S000P00','D262D000P00','D262E000P00','D262N000P00','D262T000P00','D262U000P00','D262V000P00','D262W000P00','D269C021P00','D269D021P00','D269G021P00','D244V000P00')
                then 'North Broiler'
            when entity_code = 'LFBD' and doanh_thu_draft.sub in ('D206P000P00','D267E000P00','D267E021P00','D206P000P70','D206G000P00','D206P000P20','D206P000P30','D204C000P00','D204D000P00','D204R000P00','D204S000P00','D204T000P00','D204V000P00','D204X000P00','D204Y000P00','D267C021P00','D267D021P00','D267C000P10','D204W000P00','D204Z000P10','D204W000P10','D204Z000P00','D220Z000P00')
                then 'South Broiler'
            when entity_code = 'LFBD' and doanh_thu_draft.sub in ('D217P000P70','D203P000P70','D202Q000P70','D213Q000P70','D217PM25P00','D219Q000P70','D219QM25P70','D203P000P30','D217P000P21','D217P000P22','D203P000P60','D217P000P20','D203P000P50','D217P000P50','D217PM26P00','D202QM25P50','D202QM25P70','D202QM26P70','D213QM25P51','D213QM26P70','D217P000P51','D217P000P52','D219QM25P50','D219QM25P52','D219QM26P70','D202QM25P51','D202QM25P52','D202QM26P50','D202QM26P52','D202QM26P60','D202QM26P61','D213QM26P61','D217PM25P50','D217PM25P52','D217PM26P50','D217PM26P52','D217PM26P60','D217PM26P62','D219QM25P51','D219QM26P50','D219QM26P52','D219QM26P60','D219QM26P62','D217PA04P70','D203P000P00','D203P000P20')
                then 'Breeder'
            when entity_code = 'CM' then 'Cam My'
        end as coststructure_farmname,

        case when coststructure_farmname in ('Hung Yen','Hung Yen Semen','Bac Can 1','Bac Can 1 Semen','Yen Bai','Bac Can 2') then 'North 1'
            when coststructure_farmname in ('Hoa Binh','Thanh Hoa 2','Thanh Hoa 1','Thanh Hoa 3') then 'North 2'
            when coststructure_farmname in ('Gia Lai 1','Dak Lak 1','Gia Lai 2','Gia Lai 5','Gia Lai 6','Quang Nam 1','Phu Yen') then 'Central'
            when coststructure_farmname in ('Binh Thuan','Dong Nam Bo 2','Dong Nam Bo 1','Lang Viet Nam','Lang Viet 1','Lang Viet 2') then 'South 1'
            when coststructure_farmname in ('Cujut','Tay Ninh 1','Tay Ninh 2','Ca Mau 1','Tay Ninh 3','Tay Ninh 4','Dak Nong 2','Cam My') then 'South 2'
            when coststructure_farmname = 'North Broiler' then 'North-Broiler'
            when coststructure_farmname = 'South Broiler' then 'South-Broiler'
            when coststructure_farmname = 'Breeder' then 'South-Breeder'
        end as bu_type_2,

        case when bu_type_2 like 'North%%' then 'North'
            when bu_type_2 like 'South%%' then 'South'
            when bu_type_2 like 'Oversea' then 'Oversea'
            when bu_type_2 like 'Central' then 'Central'
        end as region_1,

        case when region_1 in ('Central','North') then 'North-Central'
            when region_1 = 'Oversea' then 'Oversea'
            when region_1 = 'South' then 'South'
        end as region_2,

        case when doanh_thu_draft.seg4_2 = 'D' then 'Swine'
            when doanh_thu_draft.seg4_2 = 'P' then 'Poultry'
        end as product_type,

        case when industry = 'Swine' then qty_in + attackqty end as quantity_swine

    from doanh_thu_draft
    left join {{ ref("farm_stg_excel__farm_pg") }} farm_pg on doanh_thu_draft.seg4 = farm_pg.pg
    
)

select * from doanh_thu

