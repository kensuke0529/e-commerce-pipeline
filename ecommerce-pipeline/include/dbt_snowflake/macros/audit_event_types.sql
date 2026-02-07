{% macro audit_event_types() %}
    {% set query %}
        SELECT DISTINCT event_data:event_type::string as event_type FROM DBT_DEMO_SNOWFLAKE.DBT.RAW_EVENTS
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {{ log("Unique Event Types in RAW_EVENTS:", info=True) }}
        {% for row in results %}
            {{ log(row[0], info=True) }}
        {% endfor %}
    {% endif %}
{% endmacro %}
