{{ config(
    materialized='view'
) }}

with staging_events as (
    select * from {{ ref('stg_events') }}
),

flattened as (
    select
        stg.event_id,
        stg.order_id,
        stg.customer_id,
        stg.event_at,
        stg.event_type,
        

        -- 'items' is the alias for the lateral flatten result
        items.value:product:id::string as product_id,
        items.value:product:name::string as product_name,
        items.value:product:category::string as category,
        items.value:product:price::float as unit_price,
        items.value:quantity::int as quantity,
        
    from staging_events as stg,
    -- This turns the array into individual rows
    lateral flatten(input => order_items_array) items
)

select * from flattened