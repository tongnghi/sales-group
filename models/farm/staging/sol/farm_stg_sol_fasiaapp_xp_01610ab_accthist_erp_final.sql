{{
    config(
        materialized="table",
    )
}}

select
begpernbr,
periodactivityneg,
sub,
cpnyid,
periodactivitypos,
legal_name,
dramttot,
endingbalance,
ledgerid,
acct,
legal,
cramttot,
endpernbr,
begbal,
balancetype,
startingbalance,
fiscyr
  from {{ source("farm_sol_fasiaapp", "xp_01610ab_accthist_erp_final") }}