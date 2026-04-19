{% materialization insert_by_period, adapter='sqlserver' -%}
  {%- set timestamp_field = config.require('timestamp_field') -%}
  {%- set backfill = config.get('backfill') or False -%}
  {% set start_date = var('min_date', '1900-01-01') %}
  {% set stop_date = var('max_date', '2099-12-31') %}

  {%- if sql.find('__PERIOD_FILTER__') == -1 -%}
    {%- set error_message -%}
      Model '{{ model.unique_id }}' does not include the required string '__PERIOD_FILTER__' in its sql
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
  {%- endif -%}

  {%- set identifier = model['name'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(database=database, identifier=identifier, schema=schema, type='table') -%}

  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_not_as_table = (old_relation is not none and not old_relation.is_table) -%}

  {%- set should_truncate = (non_destructive_mode and full_refresh_mode and exists_as_table) -%}
  {%- set should_drop = (not should_truncate and (full_refresh_mode or exists_not_as_table)) -%}
  {%- set force_create = (flags.FULL_REFRESH and not flags.NON_DESTRUCTIVE) -%}

  -- setup
  {% if old_relation is none -%}
    -- noop
  {%- elif should_truncate -%}
    {{adapter.truncate_relation(old_relation)}}
  {%- elif should_drop -%}
    {{adapter.drop_relation(old_relation)}}
    {%- set old_relation = none -%}
  {%- endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% if force_create or old_relation is none -%}
    {# Create an empty target table -#}
    {% call statement('main') -%}
      {%- set empty_sql = sql | replace("__PERIOD_FILTER__", '1=0') -%}
      {{create_table_as(False, target_relation, empty_sql)}}
    {%- endcall %}
  {%- endif %}

  {% set period_boundaries = get_period_boundaries(
    schema,
    identifier,
    timestamp_field,
    start_date,
    stop_date,
    backfill,
    full_refresh_mode,
  ) %}
  {% set period_boundaries_results = load_result('period_boundaries')['data'][0] %}
  {%- set start_timestamp = period_boundaries_results[0] | string -%}
  {%- set stop_timestamp = period_boundaries_results[1] | string -%}

  {% set target_columns = adapter.get_columns_in_relation(target_relation) %}

  -- filter out id column that is created via post hook
  {%- set excluded_column_name = model['name'] ~ '_id' -%}
  {%- set filtered_columns = target_columns | rejectattr('name', 'equalto', excluded_column_name) | list -%}
  {%- set target_cols_csv = filtered_columns | map(attribute='quoted') | join(', ') -%}


  -- commit each period as a separate transaction

  {%- set tmp_identifier = model['name'] ~ '_tmp' -%}
  {%- set tmp_relation = create_relation_for_insert_by_period(tmp_identifier, database, schema, 'view') -%}
  {% call statement() -%}
    {% set tmp_table_sql = get_period_sql(target_cols_csv,
                                                    sql,
                                                    timestamp_field,
                                                    start_timestamp,
                                                    stop_timestamp
                                                    ) %}
    {{dbt.create_view_as(tmp_relation, tmp_table_sql)}}
  {%- endcall %}

  {{adapter.expand_target_column_types(from_relation=tmp_relation,
                                        to_relation=target_relation)}}
  {% call statement('main_insert', fetch_result=True) -%}
    insert into {{target_relation}} WITH (TABLOCK) ({{target_cols_csv}})
    (
        select
            {{target_cols_csv}}
        from {{tmp_relation.include(schema=True)}}
    );
  {%- endcall %}

  {% set result = load_result('main_insert') %}
  {% set rows_inserted = get_rows_inserted(result) %}

  -- drop tmp view
  {{ drop_relation(tmp_relation) }}


  -- from the table mat
  {% do create_indexes(target_relation) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
  -- end from the table mat

  {%- set status_string = "INSERT " ~ rows_inserted -%}

  {% call noop_statement('main', status_string) -%}
    -- no-op
  {%- endcall %}

  -- Return the relations created in this materialization
  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}