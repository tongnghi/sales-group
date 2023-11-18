

  create view "food"."nghi_dev"."food_stg_sap_s4__md_0costcenter_text__dbt_tmp" as (
    -- TODO: add dedup mechanism, ex. truncate before loading, or use macro (get lastest
-- row), ...
with
    selected_fields as (
        select kostl as code, txtsh as short_name, txtmd as medium_name
        from "food"."stg_sap_s4"."md_0costcenter_text"
        where langu = 'E' and kokrs = '1000'
    ),

    deduped as (
        with row_numbered as (
        select
            _inner.*,
            row_number() over (
                partition by code
                order by code asc
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
