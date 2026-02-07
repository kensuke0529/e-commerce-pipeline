{{ config(
    materialized='table',
    tags=['bi_ready', 'analytics']
) }}

/*
    BI-Ready Orders Table
    
    Purpose: Pre-joined order data with customer and session context
    Use Case: BI tools can query this directly without joins
    
    Grain: One row per order
*/

with orders as (
    select * from {{ ref('fct_orders') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

final as (
    select
        -- Order details
        o.order_id,
        o.ordered_at,
        o.revenue,
        o.tax_amount,
        o.shipping_amount,
        (o.revenue + o.tax_amount + o.shipping_amount) as total_order_value,
        
        -- Customer attributes
        o.customer_id,
        c.customer_email,
        concat(c.customer_first_name, ' ', c.customer_last_name) as customer_name,
        c.customer_first_name,
        c.customer_last_name,
        c.customer_country,
        c.customer_segment,
        c.customer_activity_status,
        c.total_orders as customer_lifetime_orders,
        c.total_spent as customer_lifetime_value,
        
        -- Session context
        o.session_id,
        o.acquisition_channel,
        o.device_type,
        
        -- Derived metrics
        case 
            when c.total_orders = 1 then true 
            else false 
        end as is_first_order,
        
        datediff(day, c.last_order_date, o.ordered_at) as days_since_previous_order,
        
        -- Time dimensions for BI filtering
        date(o.ordered_at) as order_date,
        year(o.ordered_at) as order_year,
        month(o.ordered_at) as order_month,
        quarter(o.ordered_at) as order_quarter,
        dayofweek(o.ordered_at) as order_day_of_week,
        hour(o.ordered_at) as order_hour
        
    from orders o
    left join customers c on o.customer_id = c.customer_id
)

select * from final
