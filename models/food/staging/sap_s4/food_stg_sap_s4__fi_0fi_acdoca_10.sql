{% if target.name == 'prod' %}
{{
    config(
        pre_hook="update {{ source('food_sap_s4', 'fi_0fi_acdoca_10') }} set _created_at = sysdate where _created_at is null",
        materialized="incremental",
        unique_key=["rclnt", "rldnr", "rbukrs", "gjahr", "belnr", "docln"],
        incremental_strategy="delete+insert",
    )
}}
{% endif %}

with source as (

    {% if is_incremental() %}
        with last_run as (select max(_created_at) as max_created_at from {{ this }})
    {% endif %}

    select *
    from {{ source("food_sap_s4", "fi_0fi_acdoca_10") }}
    where
        {% if target.name == 'prod' %}
            budat >= '20220101'
        {% else %}
            budat >= to_char(current_date - interval '1 day', 'YYYYMMDD')
        {% endif %}
        {% if is_incremental() %}
            and _created_at > (select max_created_at from last_run)
        {% endif %}
        
),

deduped as (
    {{
        dbt_utils.deduplicate(
            relation="source",
            partition_by="rclnt, rldnr, rbukrs, gjahr, belnr, docln",
            order_by="_created_at desc",
        )
    }}
)

select * from deduped
