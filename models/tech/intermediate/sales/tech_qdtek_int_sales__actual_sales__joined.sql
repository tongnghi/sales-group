with
    raw_data as (
        select
            dailysales.billing_number,
            dailysales.billing_date,
            dailysales.billing_type_name,

            case
                when
                    dailysales.billing_type_name = 'Nh?p hang ban tr? l?i'
                    or dailysales.billing_type_name is null
                    -- TO-DO: What the fuck is this?? :D
                    and 'Nh?p hang ban tr? l?i' is null
                then 'ZRE'
                when
                    dailysales.billing_type_name = 'Ban hang'
                    or dailysales.billing_type_name is null
                    -- TO-DO: What the fuck is this?? :D
                    and 'Ban hang' is null
                then 'ZF2'
                else null
            end as billing_type_code,

            case
                when salesman.old_sales_code = salesman.new_sales_code
                then salesman.old_sales_code
                when salesman.old_sales_code is null
                then salesman.new_sales_code
                else salesman.old_sales_code
            end as sales_code,

            dailysales.customer_group_name,
            dailysales.article_code,
            '-' as article_name,
            dailysales.article_group,
            dailysales.article_group_name,
            dailysales.article_group_code,
            dailysales.debit,
            dailysales.rebate,
            dailysales.credit,
            dailysales.discount,
            dailysales.increase,
            dailysales.cogs_debit,
            dailysales.cogs_credit,
            dailysales.customer_id,
            dailysales.customer_name,
            dailysales.salesman_id,
            dailysales.salesman_name,

            case
                when salesman.old_sales_code = salesman.new_sales_code
                then salesman.team_code
                when salesman.old_sales_code is null
                then salesman.team_code
                else salesman.team_code
            end as team_code,

            case
                when salesman.old_sales_code = salesman.new_sales_code
                then
                    case
                        when right(salesman.team_code, 2) = 'MB'
                        then 'NORTH'
                        else 'SOUTH'
                    end

                when salesman.old_sales_code is null
                then
                    case
                        when right(salesman.team_code, 2) = 'MB'
                        then 'NORTH'
                        else 'SOUTH'
                    end
                else
                    case
                        when right(salesman.team_code, 2) = 'MB'
                        then 'NORTH'
                        else 'SOUTH'
                    end
            end as region_code,

            team.team as old_team_code,
            team.region as old_region_code,
            team.industry as old_industry_code

        from {{ ref("tech_qdtek_stg_excel_sales__daily_sales") }} dailysales
        left join
            {{ ref("tech_qdtek_stg_excel_sales__salesman") }} salesman
            on dailysales.salesman_id = salesman.old_sales_code
            and dailysales.salesman_id = salesman.new_sales_code
        left join {{ ref("team") }} team on salesman.team_code = team.team
    ),

    ultimate as (
        select
            (debit - rebate - discount - credit + increase) as actual_amount,
            case
                when
                    salesman_id = 'TEKN144'
                    and split_part(billing_date, '-', 1) = '2023'
                then '612870'
                else salesman_id
            end as change_amount,
            article_group as product_code,
            *
        from raw_data
    )

select *
from ultimate
