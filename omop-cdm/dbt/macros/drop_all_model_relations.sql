{% macro drop_all_model_relations() %}
  {% for node in graph.nodes.values() %}
    {% if node.resource_type == 'model'
        and node.config.get('enabled', true)
        and node.config.get('materialized') != 'ephemeral' %}

      {% set database = node.database %}
      {% set schema = node.schema %}
      {% set name = node.name %}

      {% set relation = adapter.get_relation(
          database=database,
          schema=schema,
          identifier=name
        )
      %}

      {% if relation %}
        {{ log("Dropping relation: " ~ relation, info=True) }}
        {{ adapter.drop_relation(relation) }}
      {% else %}
        {{ log("Relation not found: " ~ database ~ '.' ~ schema ~ '.' ~ name, info=True) }}
      {% endif %}

    {% endif %}
  {% endfor %}
{% endmacro %}
