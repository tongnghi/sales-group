with
    ultimate as (
        select
            id as id,
            pipeline as pipeline_name,
            "Repeat deal" as repeat_deal_status,
            "Repeat inquiry" as repeat_inquiry_status,
            stage as stage_name,
            responsible as responsible_name,
            "Deal Name" as deal_name,
            type as type_name,
            Income,
            Currency as currency_code,
            Company as company_name,
            Contact as contact_name,
            Created as created_timestamp,
            "Created by" as created_by_name,
            Modified as modified_timestamp,
            "Modified by" as modified_by_name,
            "Start date" as start_date,
            "Assumed close date" as assumed_closed_date,
            Product as product_code,
            Price,
            Probability as percent_probability,
            Invoiced as invoiced_status,
            Industry as industry_name,
            TEAM as team_code,
            DEPT as department_code,
            BRANCH as branch_code
        from {{ source("qdtek_excel_sales", "pipeline_crm") }}
    )

select * 
from ultimate
