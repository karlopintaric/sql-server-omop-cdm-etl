{% macro create_relation_for_insert_by_period(tmp_identifier, database, schema, type) %}
    {% set relation = api.Relation.create(
        database=database,
        schema=schema,
        identifier=tmp_identifier,
        type=type
    ) %}

    {% set existing_relation = adapter.get_relation(
        database=database,
        schema=schema,
        identifier=tmp_identifier
    ) %}

    {% if existing_relation is not none %}
        {{ log("Dropping existing relation: " ~ relation, info=True) }}
        {% do adapter.drop_relation(existing_relation) %}
    {% endif %}

    {% do return(relation) %}
{% endmacro %}
