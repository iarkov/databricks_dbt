{% macro cents_to_dollars(collumn_name, decimals=2) -%}
    ROUND({{ collumn_name }} / 100, {{ decimals }})
{%- endmacro %}