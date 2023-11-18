with tb_farm as (
    
    select
        tb,
        bu_type_2,
        coststructure_farmname,
        product_type,
        region_1,
        region_2,
        "Fixed Assets","Prepaid Expenses","Raw Materials", "Finished Goods","Tools & Supplies","Account Receivables","Acount Payables","Cash & Cash Equivalents"
 
    from {{ ref("farm_int_tb__unioned") }} 
    pivot ( sum(ending_balance) for type_account in ('Fixed Assets','Prepaid Expenses','Raw Materials', 'Finished Goods','Tools & Supplies','Account Receivables','Acount Payables','Cash & Cash Equivalents') )
 
), 

net_sales as (

    select
        entity_code as tb,
        bu_type_2,
        coststructure_farmname,
        product_type,
        region_1,
        region_2,
        sum(amount_internal) as net_sales

    from {{ ref ("farm_int_farm_sales") }}
    group by entity_code,
            bu_type_2,
            region_1,
            region_2,
            product_type,
            coststructure_farmname
    
),

type_costs as (

    select
        tb_farm.tb,
        tb_farm.bu_type_2,
        tb_farm.coststructure_farmname,
        tb_farm.product_type,
        tb_farm.region_1,
        tb_farm.region_2,
        "Fixed Assets" as fixed_assets,
        "Prepaid Expenses" as prepaid_expenses,
        "Raw Materials" as raw_materials,
        "Finished Goods" as finished_goods,
        "Tools & Supplies" as tools_supplies,
        "Account Receivables" as account_receivables,
        "Acount Payables" as account_payables,
        "Cash & Cash Equivalents" as cash_cashequivalents,
        fixed_assets + prepaid_expenses + raw_materials + finished_goods + tools_supplies + account_receivables + account_payables + cash_cashequivalents as total_assets,
        net_sales.net_sales

    from tb_farm
    left join net_sales on tb_farm.tb = net_sales.tb 
                        and tb_farm.bu_type_2 = net_sales.bu_type_2
                        and tb_farm.coststructure_farmname = net_sales.coststructure_farmname
                        and tb_farm.product_type = net_sales.product_type
                        and tb_farm.region_1 = net_sales.region_1
                        and tb_farm.region_2 = net_sales.region_2

),

funcdept as (

    select
        t1.*,

        case when t1.product_type in ('Swine', 'Poultry') and region_2 in ('South','North-Central','Oversea') then t1.net_sales end as "Func.Dept_Group?Expense",

        case when t1.product_type in ('Swine', 'Poultry') and region_1 = 'North' then t1.net_sales end as "Func.Dept_Group North?Expense",

        case when t1.product_type in ('Swine', 'Poultry') and region_2 = 'North-Central' then t1.net_sales end as "Func.Dept_Group North-Central?Expense",

        case when t1.product_type in ('Swine', 'Poultry') and region_2 = 'North-Central' then t1.total_assets end as "Func.Dept_Group North-Central?Finance",

        case when t1.product_type in ('Swine', 'Poultry') and region_2 = 'South' then t1.net_sales end as "Func.Dept_Group South?Expense",

        case when t1.product_type in ('Swine', 'Poultry') and region_2 = 'South' then t1.total_assets end as "Func.Dept_Group South?Finance",

        case when t1.product_type in ('Swine') and region_2 in ('South','North-Central','Oversea') then t1.net_sales end as "Func.Dept_Swine Group?Expense",

        case when t1.product_type in ('Swine') and region_2 in ('North-Central') then t1.net_sales end as "Func.Dept_Swine North-Central?Expense",

        case when t1.product_type in ('Swine') and region_2 in ('North-Central') then t1.total_assets end as "Func.Dept_Swine North-Central?Finance",

        case when t1.product_type in ('Swine') and region_2 in ('South') then t1.net_sales end as "Func.Dept_Swine South?Expense",

        case when t1.product_type in ('Swine') and region_2 in ('South') then t1.total_assets end as "Func.Dept_Swine South?Finance",

        case when t1.product_type in ('Swine') and region_1 in ('Central') then t1.net_sales end as "Func.Dept_BU Swine Central?Expense",

        case when t1.product_type in ('Swine') and bu_type_2 in ('North 1') then t1.net_sales end as "Func.Dept_BU Swine North 1?Expense",

        case when t1.product_type in ('Swine') and bu_type_2 in ('North 2') then t1.net_sales end as "Func.Dept_BU Swine North 2?Expense",

        case when t1.product_type in ('Swine') and bu_type_2 in ('South 1') then t1.net_sales end as "Func.Dept_BU Swine South 1?Expense",

        case when t1.product_type in ('Swine') and coststructure_farmname in ('Lang Viet Nam','Lang Viet 1','Lang Viet 2') then t1.net_sales end as "Func.Dept_BU Swine South 1_Farms Lang Viet?Expense",
        
        case when t1.product_type in ('Swine') and bu_type_2 in ('South 2') then t1.net_sales end as "Func.Dept_BU Swine South 2?Expense",

        case when t1.product_type in ('Swine') and coststructure_farmname in ('Binh Thuan','Cujut','Dong Nam Bo 1','Dong Nam Bo 2') then t1.net_sales end as "Func.Dept Cujut_DNB2_BinhThuan?Expense",

        case when t1.product_type in ('Poultry') and region_1 in ('South','North') then t1.net_sales end as "Func.Dept_Poultry Group?Expense",

        case when t1.product_type in ('Poultry') and bu_type_2 in ('North-Broiler') then t1.net_sales end as "Func.Dept_Poultry North?Expense",

        case when t1.product_type in ('Poultry') and region_1 in ('South') then t1.net_sales end as "Func.Dept_Poultry South?Expense",

        case when t1.product_type in ('Poultry') and bu_type_2 in ('North-Broiler') then t1.net_sales end as "Func.Dept_BU Poultry North?Expense",

        case when t1.product_type in ('Poultry') and region_1 in ('South') then t1.net_sales end as "Func.Dept_BU Poultry South?Expense",

        case when t1.product_type in ('Poultry') and bu_type_2 in ('South-Breeder') then t1.net_sales end as "Func.Dept_Poultry Maintenance South?Expense",
       
        case when t1.product_type in ('Poultry') and bu_type_2 in ('South-Broiler') then t1.net_sales end as "Func.Dept_BU Poultry South_Broiler?Expense",

        case when t1.product_type in ('Poultry') and bu_type_2 in ('South-Breeder') then t1.net_sales end as "Func.Dept_BU Poultry South_Breeder?Expense",

        case when t1.product_type in ('Poultry') and bu_type_2 in ('North-Broiler') then t1.net_sales end as "Func.Dept_PoultryFarm_Technical_Outsource North?Expense",

        case when t1.product_type in ('Swine', 'Poultry') and region_2 in ('South','North-Central')  then t1.net_sales end as a,

        case when t1.coststructure_farmname in ('Hung Yen','Hung Yen Semen','Binh Thuan','Cujut','Dong Nam Bo 1','Dong Nam Bo 2','Cam My') then t1.net_sales end as b,

        a + b as "Func.Dept_LF Tax?Expense"

    from type_costs t1

)

select * from funcdept