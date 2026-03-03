{% macro source(source_name, table_name) %}
  {% set relation = builtins.source(source_name, table_name) %}
  {% if target.name == 'ci' %}
    (select * from {{ relation }} tablesample system (1 percent))
  {% else %}
    {{ relation }}
  {% endif %}
{% endmacro %}
