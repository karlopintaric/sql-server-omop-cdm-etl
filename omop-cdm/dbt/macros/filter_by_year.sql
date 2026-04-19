{% macro filter_by_year(date_column, required=False) %}
  {% set min_date = var('min_date', none) %}
  {% set max_date = var('max_date', none) %}

  {% if min_date and max_date %}
    {{ date_column }} >= '{{ min_date }}' AND {{ date_column }} < '{{ max_date }}'
  {% elif required %}
    {{ log("⚠️ 'min_date' and 'max_date' variables are required but not provided — skipping filter anyway.", info=True) }}
    1=1
  {% else %}
    1=1
  {% endif %}
{% endmacro %}
