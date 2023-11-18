select distinct caption, criterias, code from {{ ref("mapping_md_bs") }}
