{{ config(materialized='view') }}

with sessions as (
    select * from {{ ref('stg_events') }}
),

enriched as (
    select
        session_id,

        min(event_at) as session_start,
        max(event_at) as session_end,

        -- duration in seconds 
        datediff('second', min(event_at), max(event_at)) as session_duration_seconds,

        count(distinct event_id) as total_events,
        
        max(case when event_type = 'order_fulfilled' then 1 else 0 end) as _is_order_fulfilled,

        any_value(customer_id) as customer_id,
        any_value(customer_country) as customer_country,
        any_value(acquisition_channel) as acquisition_channel,
        any_value(device_type) as device_type,
        any_value(order_id) as order_id,
        any_value(total_amount) as total_amount

    from sessions
    group by session_id
)

select * from enriched