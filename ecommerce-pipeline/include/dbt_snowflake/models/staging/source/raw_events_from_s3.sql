{{ config(
    materialized='incremental',
    unique_key=['source_file', 'file_row_number'],
    on_schema_change='append_new_columns',
    alias='RAW_EVENTS'
) }}

/*
    This model loads raw event data from S3 into Snowflake.
    It reads from the external stage and tracks which files have been processed.
    
    Run this model first: dbt run --select raw_events_from_s3
    Then run stg_events to transform the data.
*/

with s3_files as (
    select
        $1 as event_data,
        metadata$filename as source_file,
        metadata$file_row_number as file_row_number,
        current_timestamp() as loaded_at
    from @DBT_DEMO_SNOWFLAKE.DBT.s3_raw_events_stage
    
    {% if is_incremental() %}
    
    -- Only load new files 
    where metadata$filename not in (
        select distinct source_file 
        from {{ this }}
    )
    {% endif %}
)

select * from s3_files
