select code, name, _source from {{ ref("food_int_channels__unioned") }}
