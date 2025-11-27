{% macro cents_to_dollars(collumn_name, decimals=2) -%}
    round(cast({{ collumn_name }} as float) / 100, {{ decimals }})
{%- endmacro %}