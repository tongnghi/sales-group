{{
    config(
        materialized="table",
    )
}}

with
    raw_fi as (
        select gl.*, debit - credit as accumulated_balance
        from {{ ref("food_stg_sap_s4__fi_0fi_gl_12") }} gl
        where
            curtype = '10'
            and valuetype = '010'
            and kokrs = '1000'
            and chartaccts = '1000'
            and rbukrs = '3000'
    ),
    accumulated as (
        select a.period, b.*
        from {{ ref("consol_fiscal_period") }} as a
        cross join raw_fi as b
        where b.fiscper <= a.period and "left"(b.fiscper, 4) = "left"(a.period, 4)
    ),

    final as (
        select
            period,
            racct as gl_account,
            sum(
                case
                    when
                        racct >= '0011100000'
                        and racct <= '0011199999'
                        or racct >= '0011200000'
                        and racct <= '0011299999'
                        or racct >= '0011300000'
                        and racct <= '0011399999'
                        or racct >= '0012111000'
                        and racct <= '0012111000'
                        or racct >= '0012121000'
                        and racct <= '0012121000'
                    then accumulated_balance
                end
            ) as "111",
            sum(
                case
                    when
                        racct >= '0012811000'
                        and racct <= '0012811000'
                        or racct >= '0012812000'
                        and racct <= '0012812000'
                        or racct >= '0012813000'
                        and racct <= '0012813000'
                        or racct >= '0012818000'
                        and racct <= '0012818000'
                        or racct >= '0012819099'
                        and racct <= '0012819099'
                        or racct >= '0012814000'
                        and racct <= '0012814000'
                    then accumulated_balance
                end
            ) as "112",
            --"111" + "112" as "110",
            sum(
                case
                    when racct >= '0012100000' and racct <= '0012199999'
                    then accumulated_balance
                end
            ) as "121",
            sum(
                case
                    when racct >= '0022910000' and racct <= '0022910000'
                    then accumulated_balance
                end
            ) as "122",
            sum(
                case
                    when racct >= '0012881000' and racct <= '0012881000'
                    then accumulated_balance
                end
            ) as "123",
            --"121" + "122" + "123" as "120",
            sum(
                case
                    when racct >= '0013100000' and racct <= '0013119999'
                    then accumulated_balance
                end
            ) as "131",
            sum(
                case
                    when
                        racct >= '0024218004'
                        and racct <= '0024218004'
                        or racct >= '0033120000'
                        and racct <= '0033129999'
                    then accumulated_balance
                end
            ) as "132",
            sum(
                case
                    when racct >= '0013600000' and racct <= '0013699999'
                    then accumulated_balance
                end
            ) as "133",
            sum(
                case
                    when racct >= '0013499999' and racct <= '0013499999'
                    then accumulated_balance
                end
            ) as "134",
            sum(
                case
                    when racct >= '0013599999' and racct <= '0013599999'
                    then accumulated_balance
                end
            ) as "135",
            sum(
                case
                    when
                        racct >= '0014100000'
                        and racct <= '0014199999'
                        or racct >= '0013851000'
                        and racct <= '0013887299'
                        or racct >= '0013889100'
                        and racct <= '0013889999'
                        or racct >= '0024411002'
                        and racct <= '0024419001'
                        and racct <> '0024411001'
                    then accumulated_balance
                end
            ) as "136",
            sum(
                case
                    when
                        racct >= '0022931001'
                        and racct <= '0022931001'
                        or racct >= '0022931002'
                        and racct <= '0022931002'
                        or racct >= '0022931003'
                        and racct <= '0022931003'
                        or racct >= '0022931008'
                        and racct <= '0022931008'
                        or racct >= '0022961000'
                        and racct <= '0022961000'
                    then accumulated_balance
                end
            ) as "137",
            sum(
                case
                    when
                        racct >= '0013811000'
                        and racct <= '0013811000'
                        or racct >= '0013812000'
                        and racct <= '0013812000'
                    then accumulated_balance
                end
            ) as "139",
            --"131" + "132" + "133" + "134" + "135" + "136" + + "137" + "139" as "130",
            sum(
                case
                    when
                        racct >= '0015100000'
                        and racct <= '0015199999'
                        or racct >= '0015200000'
                        and racct <= '0015299999'
                        or racct >= '0015300000'
                        and racct <= '0015399999'
                        or racct >= '0015400000'
                        and racct <= '0015499999'
                        or racct >= '0015500000'
                        and racct <= '0015599999'
                        or racct >= '0015600000'
                        and racct <= '0015699999'
                        or racct >= '0015431000'
                        and racct <= '0015431000'
                    then accumulated_balance
                end
            ) as "141",
            sum(
                case
                    when
                        racct >= '0022941000'
                        and racct <= '0022941000'
                        or racct >= '0022948000'
                        and racct <= '0022948000'
                    then accumulated_balance
                end
            ) as "149",
            --"141" + "149" as "140",
            sum(
                case
                    when
                        racct >= '0024210000'
                        and racct <= '0024219999'
                        and racct <> '0024218004'
                    then accumulated_balance
                end
            ) as "151",
            sum(
                case
                    when racct >= '0013300000' and racct <= '0013399999'
                    then accumulated_balance
                end
            ) as "152",
            sum(
                case
                    when
                        racct >= '0013887300'
                        and racct <= '0013887300'
                        or racct >= '0013887400'
                        and racct <= '0013887400'
                        or racct >= '0013887500'
                        and racct <= '0013887500'
                        or racct >= '0013887800'
                        and racct <= '0013887800'
                        or racct >= '0013887899'
                        and racct <= '0013887899'
                    then accumulated_balance
                end
            ) as "153",
            sum(
                case
                    when racct >= '0015499999' and racct <= '0015499999'
                    then accumulated_balance
                end
            ) as "154",
            sum(
                case
                    when racct >= '0015599999' and racct <= '0015599999'
                    then accumulated_balance
                end
            ) as "155",
           -- "151" + "152" + "153" + "154" + "155" as "150",
            --"110" + "120" + "130" + "140" + "150" as "100",
            sum(
                case
                    when racct >= '0021199999' and racct <= '0021199999'
                    then accumulated_balance
                end
            ) as "211",
            sum(
                case
                    when racct >= '0033146000' and racct <= '0033146000'
                    then accumulated_balance
                end
            ) as "212",
            sum(
                case
                    when racct >= '0021399999' and racct <= '0021399999'
                    then accumulated_balance
                end
            ) as "213",
            sum(
                case
                    when racct >= '0021499999' and racct <= '0021499999'
                    then accumulated_balance
                end
            ) as "214",
            sum(
                case
                    when racct >= '0021599999' and racct <= '0021599999'
                    then accumulated_balance
                end
            ) as "215",
            sum(
                case
                    when
                        racct >= '0024420000'
                        and racct <= '0024429999'
                        or racct >= '0024411001'
                        and racct <= '0024411001'
                    then accumulated_balance
                end
            ) as "216",
            sum(
                case
                    when
                        racct >= '0022932001'
                        and racct <= '0022932001'
                        or racct >= '0022932002'
                        and racct <= '0022932002'
                        or racct >= '0022932003'
                        and racct <= '0022932003'
                        or racct >= '0022932008'
                        and racct <= '0022932008'
                        or racct >= '0022962000'
                        and racct <= '0022962000'
                    then accumulated_balance
                end
            ) as "219",
            --"211" + "212" + "213" + "214" + "215" + "216" + "219" as "210",
            sum(
                case
                    when racct >= '0021100000' and racct <= '0021199999'
                    then accumulated_balance
                end
            ) as "222",
            sum(
                case
                    when racct >= '0021410000' and racct <= '0021419999'
                    then accumulated_balance
                end
            ) as "223",
            sum(
                case
                    when racct >= '0021200000' and racct <= '0021299999'
                    then accumulated_balance
                end
            ) as "225",
            sum(
                case
                    when racct >= '0021420000' and racct <= '0021429999'
                    then accumulated_balance
                end
            ) as "226",
            sum(
                case
                    when
                        racct >= '0021300000'
                        and racct <= '0021399999'
                        and racct <> '0021382000'
                    then accumulated_balance
                end
            ) as "228",
            sum(
                case
                    when racct >= '0021430000' and racct <= '0021439999'
                    then accumulated_balance
                end
            ) as "229",
            --"222" + "223" + "225" + "226" + "228" + "229" as "220",
            sum(
                case
                    when racct >= '0023199999' and racct <= '0023199999'
                    then accumulated_balance
                end
            ) as "231",
            sum(
                case
                    when racct >= '0023299999' and racct <= '0023299999'
                    then accumulated_balance
                end
            ) as "232",
            --"231" + "232" as "230",
            sum(
                case
                    when racct >= '0024199999' and racct <= '0024199999'
                    then accumulated_balance
                end
            ) as "241",
            sum(
                case
                    when racct >= '0024110000' and racct <= '0024139999'
                    then accumulated_balance
                end
            ) as "242",
            --"241" + "242" as "240",
            sum(
                case
                    when racct >= '0022100000' and racct <= '0022199999'
                    then accumulated_balance
                end
            ) as "251",
            sum(
                case
                    when racct >= '0022200000' and racct <= '0022299999'
                    then accumulated_balance
                end
            ) as "252",
            sum(
                case
                    when racct >= '0022800000' and racct <= '0022899999'
                    then accumulated_balance
                end
            ) as "253",
            sum(
                case
                    when
                        racct >= '0022921000'
                        and racct <= '0022921000'
                        or racct >= '0022928000'
                        and racct <= '0022928000'
                        or racct >= '0033560006'
                        and racct <= '0033560006'
                        or racct >= '0022900002'
                        and racct <= '0022900002'
                    then accumulated_balance
                end
            ) as "254",
            sum(
                case
                    when
                        racct >= '0022599999'
                        and racct <= '0022599999'
                        or racct >= '0012882000'
                        and racct <= '0012882000'
                    then accumulated_balance
                end
            ) as "255",
            --"251" + "252" + "253" + "254" + "255" as "250",
            sum(
                case
                    when racct >= '0024220000' and racct <= '0024229999'
                    then accumulated_balance
                end
            ) as "261",
            sum(
                case
                    when racct >= '0024300000' and racct <= '0024399999'
                    then accumulated_balance
                end
            ) as "262",
            sum(
                case
                    when racct >= '0026399999' and racct <= '0026399999'
                    then accumulated_balance
                end
            ) as "263",
            sum(
                case
                    when racct >= '0026899999' and racct <= '0026899999'
                    then accumulated_balance
                end
            ) as "268",
            sum(
                case
                    when racct >= '0021382000' and racct <= '0021382000'
                    then accumulated_balance
                end
            ) as "269",
            --"261" + "262" + "263" + "268" + "269" as "260",
           -- "210" + "220" + "230" + "240" + "250" + "260" as "200",
           -- "100" + "200" as "270",
            sum(
                case
                    when
                        racct >= '0033881001'
                        and racct <= '0033881001'
                        or racct >= '0033881011'
                        and racct <= '0033881011'
                        or racct >= '0033111100'
                        and racct <= '0033111100'
                        or racct >= '0033111200'
                        and racct <= '0033111200'
                        or racct >= '0033112000'
                        and racct <= '0033112000'
                        or racct >= '0033113000'
                        and racct <= '0033113000'
                        or racct >= '0033114000'
                        and racct <= '0033114000'
                        or racct >= '0033115000'
                        and racct <= '0033115000'
                        or racct >= '0033116000'
                        and racct <= '0033116000'
                        or racct >= '0033116099'
                        and racct <= '0033116099'
                        or racct >= '0033119000'
                        and racct <= '0033119000'
                        or racct >= '0033119001'
                        and racct <= '0033119001'
                        or racct >= '0033119099'
                        and racct <= '0033119099'
                        or racct >= '0033191000'
                        and racct <= '0033191000'
                        or racct >= '0033116100'
                        and racct <= '0033116100'
                        or racct >= '0033112001'
                        and racct <= '0033112001'
                        or racct >= '0033116001'
                        and racct <= '0033116001'
                        or racct >= '0033116002'
                        and racct <= '0033116002'
                        or racct >= '0033116003'
                        and racct <= '0033116003'
                        or racct >= '0033116004'
                        and racct <= '0033116004'
                        or racct >= '0033116005'
                        and racct <= '0033116005'
                    then accumulated_balance
                end
            ) as "311",
            sum(
                case
                    when racct >= '0013120000' and racct <= '0013129999'
                    then accumulated_balance
                end
            ) as "312",
            sum(
                case
                    when racct >= '0033300000' and racct <= '0033399999'
                    then accumulated_balance
                end
            ) as "313",
            sum(
                case
                    when racct >= '0033400000' and racct <= '0033499999'
                    then accumulated_balance
                end
            ) as "314",
            sum(
                case
                    when
                        racct >= '0033500000'
                        and racct <= '0033599999'
                        and racct <> '0033550004'
                        and racct <> '0033560006'
                    then accumulated_balance
                end
            ) as "315",
            sum(
                case
                    when
                        racct >= '0033600000'
                        and racct <= '0033699999'
                        and racct <> '0033611000'
                    then accumulated_balance
                end
            ) as "316",
            sum(
                case
                    when racct >= '0031799999' and racct <= '0031799999'
                    then accumulated_balance
                end
            ) as "317",
            sum(
                case
                    when
                        racct >= '0033871000'
                        and racct <= '0033871000'
                        or racct >= '0033872000'
                        and racct <= '0033872000'
                    then accumulated_balance
                end
            ) as "318",
            sum(
                case
                    when
                        racct >= '0033810000'
                        and racct <= '0033869999'
                        or racct >= '0033881002'
                        and racct <= '0033889999'
                        or racct >= '0034410000'
                        and racct <= '0034419999'
                        and racct <> '0033881011'
                    then accumulated_balance
                end
            ) as "319",
            sum(
                case
                    when
                        racct >= '0034111100'
                        and racct <= '0034111999'
                        or racct >= '0034121000'
                        and racct <= '0034121999'
                    then accumulated_balance
                end
            ) as "320",
            sum(
                case
                    when
                        racct >= '0035241000'
                        and racct <= '0035241000'
                        or racct >= '0035241100'
                        and racct <= '0035241100'
                    then accumulated_balance
                end
            ) as "321",
            sum(
                case
                    when racct >= '0035300000' and racct <= '0035399999'
                    then accumulated_balance
                end
            ) as "322",
            sum(
                case
                    when racct >= '0032399999' and racct <= '0032399999'
                    then accumulated_balance
                end
            ) as "323",
            sum(
                case
                    when racct >= '0032499999' and racct <= '0032499999'
                    then accumulated_balance
                end
            ) as "324",
            {# "311"
            + "312"
            + "313"
            + "314"
            + "315"
            + "316"
            + "317"
            + "318"
            + "319"
            + "320"
            + "321"
            + "322"
            + "323"
            + "324" as "310", #}
            sum(
                case
                    when
                        racct >= '0033131000'
                        and racct <= '0033131000'
                        or racct >= '0033141000'
                        and racct <= '0033141000'
                        or racct >= '0033136000'
                        and racct <= '0033136000'
                    then accumulated_balance
                end
            ) as "331",
            sum(
                case
                    when racct >= '0033299999' and racct <= '0033299999'
                    then accumulated_balance
                end
            ) as "332",
            sum(
                case
                    when racct >= '0033399999' and racct <= '0033399999'
                    then accumulated_balance
                end
            ) as "333",
            sum(
                case
                    when racct >= '0033611000' and racct <= '0033611000'
                    then accumulated_balance
                end
            ) as "334",
            sum(
                case
                    when racct >= '0033599999' and racct <= '0033599999'
                    then accumulated_balance
                end
            ) as "335",
            sum(
                case
                    when racct >= '0033699999' and racct <= '0033699999'
                    then accumulated_balance
                end
            ) as "336",
            sum(
                case
                    when racct >= '0034420000' and racct <= '0034429999'
                    then accumulated_balance
                end
            ) as "337",
            sum(
                case
                    when
                        racct >= '0034112000'
                        and racct <= '0034112999'
                        or racct >= '0034122000'
                        and racct <= '0034122999'
                        or racct >= '0034311000'
                        and racct <= '0034319999'
                    then accumulated_balance
                end
            ) as "338",
            sum(
                case
                    when racct >= '0033999999' and racct <= '0033999999'
                    then accumulated_balance
                end
            ) as "339",
            sum(
                case
                    when racct >= '0034099999' and racct <= '0034099999'
                    then accumulated_balance
                end
            ) as "340",
            sum(
                case
                    when racct >= '0034700000' and racct <= '0034799999'
                    then accumulated_balance
                end
            ) as "341",
            sum(
                case
                    when
                        racct >= '0033550004'
                        and racct <= '0033550004'
                        or racct >= '0035220000'
                        and racct <= '0035220000'
                        or racct >= '0035230000'
                        and racct <= '0035230000'
                        or racct >= '0035242100'
                        and racct <= '0035242100'
                    then accumulated_balance
                end
            ) as "342",
            sum(
                case
                    when racct >= '0034399999' and racct <= '0034399999'
                    then accumulated_balance
                end
            ) as "343",
            {# "331"
            + "332"
            + "333"
            + "334"
            + "335"
            + "336"
            + "337"
            + "338"
            + "339"
            + "340"
            + "341"
            + "342"
            + "343" as "330", #}

            sum(
                case
                    when racct >= '0041111000' and racct <= '0041111000'
                    then accumulated_balance
                end
            ) as "411a",
            sum(
                case
                    when racct >= '0041112000' and racct <= '0041112000'
                    then accumulated_balance
                end
            ) as "411b",
            --"411a" + "411b" as "411",
            sum(
                case
                    when racct >= '0041121000' and racct <= '0041121000'
                    then accumulated_balance
                end
            ) as "412",
            sum(
                case
                    when racct >= '0041399999' and racct <= '0041399999'
                    then accumulated_balance
                end
            ) as "413",
            sum(
                case
                    when racct >= '0041181000' and racct <= '0041181000'
                    then accumulated_balance
                end
            ) as "414",
            sum(
                case
                    when racct >= '0041911000' and racct <= '0041911000'
                    then accumulated_balance
                end
            ) as "415",
            sum(
                case
                    when
                        racct >= '0041210000'
                        and racct <= '0041210000'
                        or racct >= '0041220000'
                        and racct <= '0041220000'
                        or racct >= '0041230000'
                        and racct <= '0041230000'
                    then accumulated_balance
                end
            ) as "416",
            sum(
                case
                    when
                        racct >= '0041311000'
                        and racct <= '0041311000'
                        or racct >= '0041321000'
                        and racct <= '0041321000'
                    then accumulated_balance
                end
            ) as "417",
            sum(
                case
                    when racct >= '0041410000' and racct <= '0041410000'
                    then accumulated_balance
                end
            ) as "418",
            sum(
                case
                    when racct >= '0041999999' and racct <= '0041999999'
                    then accumulated_balance
                end
            ) as "419",
            sum(
                case
                    when racct >= '0041810000' and racct <= '0041810000'
                    then accumulated_balance
                end
            ) as "420",
            sum(
                case
                    when racct >= '0042110000' and racct <= '0042110000'
                    then accumulated_balance
                end
            ) as "421a",
            sum(
                case
                    when
                        racct >= '0051100000'
                        and racct <= '0051199999'
                        or racct >= '0052100000'
                        and racct <= '0052199999'
                        or racct >= '0063200000'
                        and racct <= '0063299999'
                        or racct >= '0062100000'
                        and racct <= '0062199999'
                        or racct >= '0062200000'
                        and racct <= '0062299999'
                        or racct >= '0062700000'
                        and racct <= '0062799999'
                        or racct >= '0064100000'
                        and racct <= '0064199999'
                        or racct >= '0064200000'
                        and racct <= '0064299999'
                        or racct >= '0051500000'
                        and racct <= '0051599999'
                        or racct >= '0063500000'
                        and racct <= '0063599999'
                        or racct >= '0071100000'
                        and racct <= '0071199999'
                        or racct >= '0081100000'
                        and racct <= '0081199999'
                        or racct >= '0082110000'
                        and racct <= '0082199999'
                        or racct >= '0099000000'
                        and racct <= '0099999999'
                        or racct >= '0042120000'
                        and racct <= '0042120000'
                    then accumulated_balance
                end
            ) as "421b",
            --"421a" + "421b" as "421",
            sum(
                case
                    when racct >= '0042299999' and racct <= '0042299999'
                    then accumulated_balance
                end
            ) as "422",
            sum(
                case
                    when
                        racct >= '0041921000'
                        and racct <= '0041921000'
                        or racct >= '0041922000'
                        and racct <= '0041922000'
                    then accumulated_balance
                end
            ) as "429",
{# 
            "411"
            + "412"
            + "413"
            + "414"
            + "415"
            + "416"
            + "417"
            + "418"
            + "419"
            + "420"
            + "421"
            + "429" as "410", #}
            sum(
                case
                    when racct >= '0043199999' and racct <= '0043199999'
                    then accumulated_balance
                end
            ) as "431",
            sum(
                case
                    when racct >= '0043299999' and racct <= '0043299999'
                    then accumulated_balance
                end
            ) as "432"
            {# "431" + "432" as "430",
            "310" + "330" as "300",
            "410" + "430" as "400",
            "300" + "400" as "440" #}
        from accumulated
        group by period,
        gl_account
    -- racct
    )
select *
from final
