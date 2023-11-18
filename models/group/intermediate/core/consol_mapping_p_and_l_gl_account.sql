select
    code,
    gl_account_from as _gl_account_from,
    gl_account_to as _gl_account_to,
    excluded
from  {{ ref("mapping_p_and_l_account") }}
