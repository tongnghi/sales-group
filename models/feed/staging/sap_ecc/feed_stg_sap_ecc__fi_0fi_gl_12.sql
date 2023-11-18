{{
    config(
        pre_hook="delete from {{ source('feed_sap_ecc', 'fi_0fi_gl_12') }} where _created_at is not null; update {{ source('feed_sap_ecc', 'fi_0fi_gl_12') }} set _created_at = sysdate where _created_at is null",
        materialized="table",
    )
}}

select * from {{ source("feed_sap_ecc", "fi_0fi_gl_12") }}
