{% macro get_period_boundaries(target_schema, target_table, timestamp_field, start_date, stop_date, backfill, full_refresh_mode) -%}

  {# Strip table alias from timestamp_field if present #}
  {% set target_timestamp_field = timestamp_field.split('.')[-1] %}

  {% call statement('period_boundaries', fetch_result=True) -%}
    with data as (
    select
        {% if backfill and not full_refresh_mode -%}
            cast('{{start_date}}' as datetime) as start_timestamp,
        {%- else -%}
            coalesce(
              DATEADD(day, 1, max({{target_timestamp_field}})), 
              cast('{{start_date}}' as datetime)
              ) as start_timestamp,
        {%- endif %}
        cast(coalesce(nullif('{{stop_date}}', ''), '{{run_started_at.strftime("%Y-%m-%d")}}') as datetime) as stop_timestamp
      from {{adapter.quote(target_schema)}}.{{adapter.quote(target_table)}}
    )

    select
      start_timestamp,
      stop_timestamp
    from data
  {%- endcall %}

{%- endmacro %}