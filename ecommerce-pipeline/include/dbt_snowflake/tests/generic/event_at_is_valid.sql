{% test event_at_is_valid(model, column_name) %}
select 
    * 
from {{ model }} 
where
    {{ column_name }} is null or 
    {{ column_name }} > current_timestamp() or 
    {{ column_name }} < '2025-01-01'
{% endtest %}