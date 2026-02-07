{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    on_schema_change='append_new_columns'
) }}

with raw_source as (
    select * from {{ source('s3_bucket', 'RAW_EVENTS') }}
    
    {% if is_incremental() %}
    -- Process new events and late arriving events (3 day lookback)
    where event_data:event_timestamp::timestamp_ntz > (
        select dateadd('day', -3, coalesce(max(event_at), '1970-01-01'::timestamp_ntz)) from {{ this }}
    )
    {% endif %}
),

renamed as (
    select
        event_data:event_id::string as event_id,
        event_data:session_id::string as session_id,
        
        event_data:event_timestamp::timestamp_ntz as event_at,
        event_data:event_type::string as event_type,

        event_data:customer:customer_id::string as customer_id,
        event_data:customer:email::string as customer_email,
        event_data:customer:first_name::string as customer_first_name,
        event_data:customer:last_name::string as customer_last_name,
        event_data:customer:country::string as customer_country,

        event_data:order:order_id::string as order_id,
        event_data:order:status::string as order_status,
        event_data:order:currency::string as currency,
        event_data:order:total_amount::float as total_amount,
        event_data:order:tax::float as tax_amount,
        event_data:order:shipping::float as shipping_amount,

        event_data:session:channel::string as acquisition_channel,
        event_data:session:device::string as device_type,
        event_data:order:items as order_items_array,

        current_timestamp() as dbt_updated_at

    from raw_source
)

select * from renamed