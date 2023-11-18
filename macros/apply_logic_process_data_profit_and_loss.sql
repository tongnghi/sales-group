-- type_source: sap, excel, solomon
-- data: table data
-- cur_type: local currency type 99 ngoại tệ, 10 nguyên tệ
-- sub_type: loại excel naò

{% macro apply_logic_process_data_p_l(type_source,data,cur_type,sub_type) -%}
    {% if type_source == 'solomon' and cur_type == '99' %}
        with raw_data as (
                select
                    ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS number_id,
                    left(endpernbr, 4) || '0' || right(endpernbr, 2) as period,
                    legal,
                    '00' || acct as racct,
                    endpernbr,
                    begpernbr,
                    left(sub, 2) || '-' || substring(sub, 3, 3) as profit_center,
                    sub,
                    '10'::text as curtype,
                    nvl(a.dramttot,0) - nvl(a.cramttot,0) as _balance,
                    dramttot as debit,
                    cramttot as credit
                from {{ data }} as a
            union all
                select
                    ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS number_id,
                    left(endpernbr, 4) || '0' || right(endpernbr, 2) as period,
                    legal,
                    '00' || acct as racct,
                    endpernbr,
                    begpernbr,
                    left(sub, 2) || '-' || substring(sub, 3, 3) as profit_center,
                    sub,
                    '99'::text as curtype,
                    nvl(a.dramttot,0) - nvl(a.cramttot,0) as _balance,
                    dramttot as debit,
                    cramttot as credit
                from {{ data }} as a
                ),

     {% elif type_source == 'solomon' and cur_type == '10' %}
           with raw_data as (
                select
                    ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS number_id,
                    left(endpernbr, 4) || '0' || right(endpernbr, 2) as period,
                    legal,
                    '00' || acct as racct,
                    endpernbr,
                    begpernbr,
                    left(sub, 2) || '-' || substring(sub, 3, 3) as profit_center,
                    sub,
                    '10'::text as curtype,
                    nvl(a.dramttot,0) - nvl(a.cramttot,0) as _balance,
                    dramttot as debit,
                    cramttot as credit
                from {{ data }} as a
    ),
    {% elif type_source == 'excel' and cur_type == '10' %}
        with raw_data as  (
                  select
                    row_number() over (order by (select 1)) as number_id,
                    period,
                    a._racct,
                    a._racct as racct,
                    a.company_code,
                    '{{sub_type}}'::text as legal,
                    '10'::text as curtype,
                    a.begin_debit_balance,
                    a.end_debit_balance,
                    a.begin_credit_balance,
                    a.end_credit_balance,
                    a.credit,
                    a.debit,
                    nvl(a.debit,0) - nvl(a.credit,0) as _balance
              from {{ data }} as a
              {# left join {{ ref('consol_mapping_account_pl_to_account_sap_union') }} as b #}
                {# on a._racct = b.racct_pl #}
                {# and b.sub_type = 'lbc' #}
            ), 
    {% elif (type_source == 'sap_s4' or type_source == 'sap_ecc') and cur_type == '10' %}
        with raw_data as (
            select
            row_number() over (order by (select 1)) as number_id,
            gl.*,
            fiscper as period,
            rbukrs as legal,
            nvl(debit,0) - nvl(credit,0) as _balance
            from {{ data }} gl
            where
                curtype = '10'
                and valuetype = '010'
                and kokrs = '1000'
                and chartaccts = '1000'
                --and rbukrs = '3000'
    ),

    {%endif%}

    {# excluded_ as (
        select * from {{ ref("consol_mapping_p_and_l_gl_account") }} where excluded is not null
    ),

    included as (
        select * from {{ ref("consol_mapping_p_and_l_gl_account") }} where excluded is null
    ),

    exclude_joined as (

        select *, tbl.racct || excluded_.code as exc
        from raw_data as tbl
        join
            excluded_
            on tbl.racct::bigint >= excluded_._gl_account_from
            and excluded_._gl_account_to >= tbl.racct::bigint

    ),

    include_joined as (
        select
            tbl.*,
            included.code,
            included._gl_account_from,
            included._gl_account_to,
            tbl.racct || included.code as inc

        from raw_data as tbl
        join
            included
            on tbl.racct::bigint >= included._gl_account_from
            and included._gl_account_to >= tbl.racct::bigint

    ),

        final_data as (
            select include_joined.*
            from include_joined
            where inc not in (select exc from exclude_joined)
        ) #}
    final_data as (
        select * from raw_data
    )
    select
    distinct
        number_id::text,
        period::text,
        {# code::text, #}
        legal::text,
        racct::text,
        curtype::text,
        _balance::decimal(20,2) as balance,

        {% if type_source == 'sap_ecc' %}
            prctr::text as profit_center
        {% else %}
            ''::text as profit_center
        {% endif %}

        from final_data

    {%- endmacro %}

{% macro get_data_one_legal(data) -%}

        select legal, period, curtype,racct, balance,profit_center
        from {{ data }}
{%- endmacro %}