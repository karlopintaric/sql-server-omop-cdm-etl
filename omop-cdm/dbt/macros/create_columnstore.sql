{% macro create_clustered_columnstore_index(table_name) -%}
    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {{ log("Creating clustered columnstore index...", info=True) }}

    {%- set relation = adapter.get_relation(
        database=target.database,
        schema=target.schema,
        identifier=table_name
    ) %}

    {% if relation is none %}
        {{ log("Skipping clustered columnstore creation: relation not found for " ~ table_name, info=True) }}
        {{ return('') }}
    {% endif %}

    {% set full_table = relation.render() %}

    {%- set cci_name = (relation.schema ~ '_' ~ relation.identifier ~ '_cci')
        | replace(".", "")
        | replace(" ", "")
    -%}

    {%- set sql %}
    IF EXISTS (
        SELECT 1
        FROM sys.indexes {{ information_schema_hints() }}
        WHERE name = '{{ cci_name }}'
        AND object_id = OBJECT_ID('{{ full_table }}')
    )
    BEGIN
        DROP INDEX {{ cci_name }} ON {{ full_table }}
    END

    CREATE CLUSTERED COLUMNSTORE INDEX {{ cci_name }}
    ON {{ full_table }}
    WITH (MAXDOP = 2)
    {%- endset %}

    {{ log("Executing SQL:\n" ~ sql) }}

    {% do run_query(sql) %}
{%- endmacro %}
