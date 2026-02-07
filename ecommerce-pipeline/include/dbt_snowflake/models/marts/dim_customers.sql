{{ config(
    materialized='incremental',
    unique_key='customer_id',
    on_schema_change='append_new_columns'
) }}
-- Testing Slim CI: This change should trigger workflow to run dim_customers only

with customer_history as (
    select * from {{ ref('int_customer_purchase_history') }}
),

stg_events as (
    select * from {{ ref('stg_events') }}
),

{% if is_incremental() %}
-- Get the max last_order_date from the existing table
max_order_date as (
    select coalesce(max(last_order_date), '1970-01-01') as max_date
    from {{ this }}
),

-- Find customers with new events since the last order date
updated_customers as (
    select distinct e.customer_id 
    from stg_events e
    cross join max_order_date md
    where e.event_at > md.max_date
    and e.event_type = 'order_fulfilled'
),
{% endif %}

filtered_customer_history as (
    select * from customer_history
    
    {% if is_incremental() %}
    -- Refresh customers who have had new events in the staging layer
    where customer_id in (
        select customer_id from updated_customers
    )
    {% endif %}
),

customer_segments as (
    select 
        customer_id,
        customer_email,
        customer_first_name,
        customer_last_name,
        customer_country,
        total_orders,
        total_spent,
        last_order_date,

        case
            when total_orders > 10 then 'VIP'
            when total_orders between 5 and 10 then 'Loyal'
            else 'New'
        end as customer_segment,

        case
            when last_order_date >= dateadd(day, -30, current_date) then 'Active'
            else 'Inactive'
        end as customer_activity_status,
        
        current_timestamp() as dbt_updated_at

    from filtered_customer_history
)

select * from customer_segments

