{% macro audit_dim_customers() %}
    {% set query %}
        SELECT COUNT(*) as count FROM {{ ref('dim_customers') }}
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% for row in results %}
            {{ log("Row count in dim_customers: " ~ row[0], info=True) }}
        {% endfor %}
    {% endif %}
    
     {% set query_sample %}
        SELECT * FROM {{ ref('dim_customers') }} LIMIT 1
    {% endset %}
    
    {% set sample_results = run_query(query_sample) %}
    
    {% if execute %}
        {{ log("Sample Data from dim_customers:", info=True) }}
        {% for row in sample_results %}
            {{ log(row.values(), info=True) }}
        {% endfor %}
    {% endif %}
{% endmacro %}
