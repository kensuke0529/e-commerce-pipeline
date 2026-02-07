{% macro list_stage_files() %}
    {% set query %}
        LIST @DBT_DEMO_SNOWFLAKE.DBT.s3_raw_events_stage;
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% for row in results %}
            {{ log(row.values(), info=True) }}
        {% endfor %}
    {% endif %}
{% endmacro %}
