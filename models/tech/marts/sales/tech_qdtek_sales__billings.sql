with
    ultimate as (
        select *
        from {{ ref("tech_qdtek_int_sales__po_type__grouped_by_billing_number") }}
    )

select *
from ultimate
