{{config (
    materialized='ephemeral'
)}}

with order_items as (
    select * from {{ ref('stg_events') }}
),

customer_history as (
    select
        customer_id,
        customer_email,
        customer_first_name,
        customer_last_name,
        customer_country,
        count(distinct order_id) as total_orders,
        sum(total_amount) as total_spent,
        max(event_at) as last_order_date
    from order_items
    where event_type = 'order_fulfilled'
    group by
        customer_id,
        customer_email,
        customer_first_name,
        customer_last_name,
        customer_country
)

select * from customer_history