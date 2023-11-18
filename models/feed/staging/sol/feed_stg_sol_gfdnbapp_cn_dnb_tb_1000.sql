{{
    config(
        materialized="table",
    )
}}


select 
begpernbr,
sub,
cpnyid,
legal_name,
dramttot::decimal(20,2),
endingbalance::decimal(20,2),
ledgerid,
acct,
'GFDNBAPP'::text as legal,
cramttot::decimal(20,2),
endpernbr,
balancetype,
startingbalance::decimal(20,2),
fiscyr::text
from {{ source("farm_sol_dnb2001app", "xp_01610ab_accthist_erp_final") }}
where left(sub,2) = 'C1'



