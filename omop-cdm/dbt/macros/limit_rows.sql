{% macro limit_rows() %}
    {% if target.name == 'dev' %}
        TOP {{ 10000 }}
    {% else %}
        -- No TOP clause in other environments
        {% set top_clause = '' %}
    {% endif %}
    {{ top_clause | default('') }}
{% endmacro %}
