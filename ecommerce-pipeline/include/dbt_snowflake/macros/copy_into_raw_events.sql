{% macro copy_into_raw_events() %}
    {% set query %}
        COPY INTO DBT_DEMO_SNOWFLAKE.DBT.RAW_EVENTS
        FROM @DBT_DEMO_SNOWFLAKE.DBT.s3_raw_events_stage
        FILE_FORMAT = (FORMAT_NAME = 'json_gzip_format')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = SKIP_FILE;
    {% endset %}

    {% do run_query(query) %}
    
    {{ log("Executed COPY INTO RAW_EVENTS", info=True) }}
{% endmacro %}
