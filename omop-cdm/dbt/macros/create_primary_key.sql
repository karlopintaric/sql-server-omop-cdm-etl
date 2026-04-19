{% macro create_primary_key(table_name, columns, constraint_name=None) -%}
    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {{ log("Creating primary key constraint...", info=True) }}

    {%- set relation = adapter.get_relation(
        database=target.database,
        schema=target.schema,
        identifier=table_name
    ) %}

    {% if relation is none %}
        {{ log("Skipping primary key creation: relation not found for " ~ table_name, info=True) }}
        {{ return('') }}
    {% endif %}

    {% set full_table = relation.render() %}

    {% if not constraint_name %}
        {% set constraint_name = "pk_" + local_md5(columns | join("_")) %}
    {% endif %}

    {%- set sql %}
    IF NOT EXISTS (
        SELECT 1
        FROM sys.key_constraints
        WHERE name = '{{ constraint_name }}'
        AND parent_object_id = OBJECT_ID('{{ full_table }}')
    )
    BEGIN
        ALTER TABLE {{ full_table }}
        ADD CONSTRAINT {{ constraint_name }} PRIMARY KEY NONCLUSTERED ({{ '[' + columns | join("], [") + ']' }})
    END
    {%- endset %}

    {{ log("Executing SQL:\n" ~ sql) }}

    {% do run_query(sql) %}
{%- endmacro %}
