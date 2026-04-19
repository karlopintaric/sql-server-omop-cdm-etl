{% macro create_index(table_name, columns, clustered=False, unique=False, includes=[]) -%}
    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {{ log("Generating index SQL...") }}

    {%- set relation = adapter.get_relation(
        database=target.database,
        schema=target.schema,
        identifier=table_name
    ) %}

    {% if relation is none %}
        {{ log("Skipping index creation: relation not found for " ~ table_name, info=True) }}
        {{ return('') }}
    {% endif %}

    {% set full_table = relation.render() %}

    {% if clustered %}
        {% set idx_name = "clustered_" + local_md5(columns | join("_")) %}
    {% else %}
        {% if includes | length > 0 %}
            {% set idx_name = (
                "nonclustered_"
                + local_md5(columns | join("_"))
                + "_incl_"
                + local_md5(includes | join("_"))
            ) %}
        {% else %}
            {% set idx_name = "nonclustered_" + local_md5(columns | join("_")) %}
        {% endif %}
    {% endif %}

    {%- set sql %}
    IF NOT EXISTS (
        SELECT *
        FROM sys.indexes {{ information_schema_hints() }}
        WHERE name = '{{ idx_name }}'
        AND object_id = OBJECT_ID('{{ full_table }}')
    )
    BEGIN
        CREATE
        {% if clustered %}
            {% if unique %} UNIQUE {% endif %}
            CLUSTERED INDEX {{ idx_name }}
            ON {{ full_table }} ({{ '[' + columns | join("], [") + ']' }})
        {% else %}
            NONCLUSTERED INDEX {{ idx_name }}
            ON {{ full_table }} ({{ '[' + columns | join("], [") + ']' }})
            {% if includes | length > 0 %}
                INCLUDE ({{ '[' + includes | join("], [") + ']' }})
            {% endif %}
        {% endif %}
    END
    {%- endset %}

    {{ log("Executing SQL:\n" ~ sql) }}

    {% do run_query(sql) %}
{%- endmacro %}
