select legal, company_code, company_name, currency
from {{ ref("mapping_legal_company_code_currency") }}
