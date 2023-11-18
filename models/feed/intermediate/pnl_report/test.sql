select 
        ke24.*,
        -- TO DO: check lại với user những mã sg 140 và divison khác 10,20 thì có lấy không?
        case when ke24.sales_group = '140' and ke24.division in ('10','20','30') then ke24.sales_group||'_'||ke24.division
            when ke24.sales_group = '140' and ke24.division not in ('10','20','30') then '140_other'
            else ke24.sales_group end as sg,

        {# nvl(mapping_typecktt.type_cktt,1) as type_cktt, #}
        -- TODO: Đồng nhất viết thường
        (cktt_sauhd + cktt_sauhd_d_kg) * type_cktt as cktt_sauhd_congthem,
        cktt_sauhd_congthem + discount_price + discount_price_d_kg as cktt_congthem

        {# case when salesgroup_ck.sales_group isnull then '0'
            else salesgroup_ck.sales_group  end as sales_group_loaitru #}

    from {{ ref("feed_stg_sap_ecc__ke24") }} ke24
    left join {{ ref("feed_stg_excel_pnl__mapping_ck") }} mapping_typecktt 
        on ke24.sales_group = mapping_typecktt.sales_group
        and ke24.sales_office = mapping_typecktt.sales_office
        and ke24.division = mapping_typecktt.division