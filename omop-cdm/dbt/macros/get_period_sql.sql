{% macro get_period_sql(target_cols_csv, sql, timestamp_field, start_timestamp, stop_timestamp) -%}

  {%- set period_filter -%}
    {{timestamp_field}} >=  CAST('{{start_timestamp}}' AS DATE) AND {{timestamp_field}} < CAST('{{stop_timestamp}}' AS DATE)
  {%- endset -%}

  {%- set filtered_sql = sql | replace("__PERIOD_FILTER__", period_filter) -%}

  select
    {{target_cols_csv}}
  from (
    {{filtered_sql}}
  ) target_cols

{%- endmacro %}
