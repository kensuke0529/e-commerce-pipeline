{{ config(
    materialized='table',
    tags=['bi_ready', 'analytics']
) }}

/*
    BI-Ready Customer Summary Table
    
    Purpose: One-row-per-customer metrics for customer analytics
    Use Case: Customer segmentation, cohort analysis, retention dashboards
    
    Grain: One row per customer
*/

with customers as (
    select * from {{ ref('dim_customers') }}
),

order_items as (
    select * from {{ ref('fct_order_items') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

customer_product_affinity as (
    select
        o.customer_id,
        oi.category as favorite_category,
        sum(oi.quantity) as total_products_purchased,
        count(distinct oi.product_id) as unique_products_purchased,
        row_number() over (partition by o.customer_id order by sum(oi.line_item_revenue) desc) as category_rank
    from orders o
    join order_items oi on o.order_id = oi.order_id
    group by o.customer_id, oi.category
),

top_category as (
    select
        customer_id,
        favorite_category,
        total_products_purchased,
        unique_products_purchased
    from customer_product_affinity
    where category_rank = 1
),

customer_orders as (
    select
        customer_id,
        min(ordered_at) as first_order_date,
        max(ordered_at) as last_order_date,
        count(*) as total_orders,
        sum(revenue) as total_revenue,
        avg(revenue) as avg_order_value
    from {{ ref('fct_orders') }}
    group by customer_id
),

final as (
    select
        -- Customer info
        c.customer_id,
        c.customer_email,
        concat(c.customer_first_name, ' ', c.customer_last_name) as customer_name,
        c.customer_first_name,
        c.customer_last_name,
        c.customer_country,
        
        -- Lifetime metrics
        coalesce(co.total_orders, 0) as total_orders,
        coalesce(co.total_revenue, 0) as total_spent,
        coalesce(co.avg_order_value, 0) as avg_order_value,
        
        -- Recency
        co.first_order_date,
        co.last_order_date,
        datediff(day, co.last_order_date, current_date) as days_since_last_order,
        datediff(day, co.first_order_date, co.last_order_date) as customer_lifetime_days,
        
        -- Segmentation
        c.customer_segment,
        c.customer_activity_status,
        
        -- Product affinity
        tc.favorite_category,
        coalesce(tc.total_products_purchased, 0) as total_products_purchased,
        coalesce(tc.unique_products_purchased, 0) as unique_products_purchased,
        
        -- RFM-like scoring
        case
            when datediff(day, co.last_order_date, current_date) <= 30 then 'High'
            when datediff(day, co.last_order_date, current_date) <= 90 then 'Medium'
            else 'Low'
        end as recency_score,
        
        case
            when coalesce(co.total_orders, 0) >= 10 then 'High'
            when coalesce(co.total_orders, 0) >= 5 then 'Medium'
            else 'Low'
        end as frequency_score,
        
        case
            when coalesce(co.total_revenue, 0) >= 1000 then 'High'
            when coalesce(co.total_revenue, 0) >= 500 then 'Medium'
            else 'Low'
        end as monetary_score
        
    from customers c
    left join customer_orders co on c.customer_id = co.customer_id
    left join top_category tc on c.customer_id = tc.customer_id
)

select * from final
