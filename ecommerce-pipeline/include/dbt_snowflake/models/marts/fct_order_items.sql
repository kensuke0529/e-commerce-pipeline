{{config (materialized='table')}}

with order_items_flattened as (
    select * from {{ ref('int_order_items_flattened') }}
    where event_type = 'order_fulfilled'  -- Only include items from fulfilled orders
)

select
    order_id,
    product_id,
    product_name,
    category,
    unit_price,
    quantity,
    (unit_price * quantity) as line_item_revenue
from order_items_flattened