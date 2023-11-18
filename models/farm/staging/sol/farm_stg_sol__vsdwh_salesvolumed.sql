{% set entities = {
    "dnb2001app": "2000",
    "fasiaapp": "2000",
    "arwhyapp": "2100",
    "mtr2001app": "2200",
    "mtr1001app": "2200",
    "mtr2002app": "2200",
    "cbd1001app": "5000",
} %}
{% for db, company_code in entities.items() %}
    select '{{ company_code }}' as company_code, *
    from {{ source("farm_sol_" ~ db, "vsdwh_salesvolumed") }}
    {% if not loop.last -%}
        union all
    {%- endif %}
{% endfor %}
