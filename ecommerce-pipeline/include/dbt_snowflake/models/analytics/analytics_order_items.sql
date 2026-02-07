{{ config(
    materialized='table',
    tags=['bi_ready', 'analytics']
) }}

/*
    BI-Ready Order Items Table
    
    Purpose: Line-item level analysis with complete order and customer context
    Use Case: Product analytics, category performance, basket analysis
    
    Grain: One row per order line item
*/

with order_items as (
    select * from {{ ref('fct_order_items') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

final as (
    select
        -- Product details
        oi.product_id,
        oi.product_name,
        oi.category,
        oi.unit_price,
        oi.quantity,
        oi.line_item_revenue,
        
        -- Order context
        oi.order_id,
        o.ordered_at,
        o.revenue as total_order_revenue,
        o.tax_amount as order_tax,
        o.shipping_amount as order_shipping,
        
        -- Customer attributes
        o.customer_id,
        c.customer_email,
        concat(c.customer_first_name, ' ', c.customer_last_name) as customer_name,
        c.customer_country,
        c.customer_segment,
        c.customer_activity_status,
        
        -- Session context
        o.session_id,
        o.acquisition_channel,
        o.device_type,
        
        -- Derived metrics
        round(oi.line_item_revenue / nullif(o.revenue, 0) * 100, 2) as pct_of_order_revenue,
        
        case 
            when c.total_orders = 1 then true 
            else false 
        end as is_first_order,
        
        -- Time dimensions
        date(o.ordered_at) as order_date,
        year(o.ordered_at) as order_year,
        month(o.ordered_at) as order_month,
        quarter(o.ordered_at) as order_quarter,
        dayofweek(o.ordered_at) as order_day_of_week
        
    from order_items oi
    inner join orders o on oi.order_id = o.order_id  -- Only include items from fulfilled orders
    left join customers c on o.customer_id = c.customer_id
)

select * from final
