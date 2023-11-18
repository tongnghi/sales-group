

  create view "food"."nghi_dev"."food_stg_sap_s4__md_0distr_chan_text__dbt_tmp" as (
    -- TODO: add dedup mechanism, ex. truncate before loading, or use macro (get lastest
-- row), ...
with
    selected_fields as (
        select vtweg as code, vtext as name
        from "food"."stg_sap_s4"."md_0distr_chan_text"
        where spras = 'E'
    ),

    deduped as (
        with row_numbered as (
        select
            _inner.*,
            row_number() over (
                partition by code
                order by name asc
            ) as rn
        from selected_fields as _inner
    )

    select
        distinct data.*
    from selected_fields as data
    
    natural join row_numbered
    where row_numbered.rn = 1
    )
select *
from deduped
order by code
  ) with no schema binding;
