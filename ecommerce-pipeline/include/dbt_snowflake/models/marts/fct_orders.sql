{{config (materialized='table')}}

with orders as (
    select * from {{ ref('stg_events') }}
    where event_type = 'order_fulfilled'
),

sessions as (
    select * from {{ ref('int_session_events') }}
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.event_at as ordered_at,
        o.total_amount as revenue,
        o.tax_amount,
        o.shipping_amount,
        s.session_id,
        s.acquisition_channel,
        s.device_type
    from orders o
    left join sessions s on o.session_id = s.session_id
)

select * from final