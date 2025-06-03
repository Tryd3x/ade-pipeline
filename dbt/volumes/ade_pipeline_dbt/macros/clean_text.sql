{%- macro clean_text(column) -%}
  LOWER(TRIM({{ column }}))
{%- endmacro -%}