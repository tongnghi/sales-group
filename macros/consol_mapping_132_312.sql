{% macro mapping_132_312(code, balance) -%}
   case when
        {{code}} = '132' and nvl({{balance}},1) < 0 
        then '312'
        when {{code}} = '321' and nvl({{balance}},1) < 0
        then '132'
        else {{code}}
        end as _code,
    case when 
        {{code}} = '132' and nvl({{balance}},1) < 0
        then {{balance}}::decimal(20,2) *(-1)
        when {{code}} = '321' and nvl({{balance}},1) < 0
        then {{balance}}::decimal(20,2) *(-1)
        else {{balance}}::decimal(20,2)
        end as _balance
{%- endmacro %}