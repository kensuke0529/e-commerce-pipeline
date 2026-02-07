# Data Ingestion Pipeline

This document explains the data ingestion pipeline that moves data from **AWS S3** → **Snowflake** → **dbt transformations**. The pipeline is event-driven, automatically triggered when new data arrives in S3, and orchestrated by Apache Airflow.

## Architecture Flow
![alt text](images/image-4.png)
## 1. S3 Data Landing Zone

### S3 Bucket Structure

Data files are organized by date and hour for efficient partitioning:

```
s3://ecommerce-streaming-data-0001/
  └── raw-events/
      └── 2026/02/06/18/
          ├── events_001.json.gz
          ├── events_002.json.gz
          └── events_003.json.gz
```

### Data Format

Events are stored as JSON files with the following structure:

```json
{
  "event_id": "evt_123456",
  "session_id": "sess_789012",
  "event_timestamp": "2026-02-06T18:30:00Z",
  "event_type": "order_completed",
  "customer": {
    "customer_id": "cust_001",
    "email": "user@example.com",
    "first_name": "John",
    "last_name": "Doe",
    "country": "US"
  },
  "order": {
    "order_id": "ord_456",
    "status": "fulfilled",
    "currency": "USD",
    "total_amount": 99.99,
    "tax": 8.50,
    "shipping": 5.99,
    "items": [...]
  },
  "session": {
    "channel": "organic",
    "device": "mobile"
  }
}
```


## 2. Snowflake Setup

### Storage Integration (Secure S3 Access)

Use IAM roles for secure authentication:

```sql
CREATE STORAGE INTEGRATION ECOMMERCE_S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = your_arn
  STORAGE_ALLOWED_LOCATIONS = (your_bucket_location);
```



### File Format Configuration

Define how Snowflake should parse the JSON files:

```sql
CREATE OR REPLACE FILE FORMAT json_gzip_format
  TYPE = 'JSON'
  COMPRESSION = 'GZIP'
  STRIP_OUTER_ARRAY = FALSE
  STRIP_NULL_VALUES = FALSE
  REPLACE_INVALID_CHARACTERS = TRUE
  DATE_FORMAT = 'AUTO'
  TIME_FORMAT = 'AUTO'
  TIMESTAMP_FORMAT = 'AUTO';
```

**Configuration Details**:
- `TYPE = 'JSON'`: Parse JSON data
- `COMPRESSION = 'GZIP'`: Decompress gzipped files


### External Stage

Create a stage that references the S3 bucket:

```sql
CREATE OR REPLACE STAGE s3_raw_events_stage
  STORAGE_INTEGRATION = ECOMMERCE_S3_INTEGRATION
  URL = 'your_s3_bucket_location'
  FILE_FORMAT = json_gzip_format;
```

### Raw Events Table

Create a table to store the ingested data:

```sql
CREATE OR REPLACE TABLE RAW_EVENTS (
  event_data VARIANT,                    -- Stores entire JSON object
  source_file VARCHAR(500),              -- S3 file path for lineage
  file_row_number NUMBER,                
  loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),  
  
  CONSTRAINT unique_event PRIMARY KEY (source_file, file_row_number)
);
```

**Schema Design**:
- `event_data VARIANT`: Snowflake's semi-structured data type for JSON
- `source_file` + `file_row_number`: Composite key prevents duplicates
- `loaded_at`: Timestamp 

---

## 3. Data Loading with dbt Macro

### dbt Macro: `copy_into_raw_events`

This macro executes the `COPY INTO` command to load data from S3 into Snowflake:


```sql
{% macro copy_into_raw_events() %}
    {% set query %}
        COPY INTO DBT_DEMO_SNOWFLAKE.DBT.RAW_EVENTS
        FROM @DBT_DEMO_SNOWFLAKE.DBT.s3_raw_events_stage
        FILE_FORMAT = (FORMAT_NAME = 'json_gzip_format')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = SKIP_FILE;
    {% endset %}

    {% do run_query(query) %}
    
    {{ log("Executed COPY INTO RAW_EVENTS", info=True) }}
{% endmacro %}

-- dbt run-operation copy_into_raw_events --profiles-dir /path/to/profiles
```

**Configuration Details**:
- `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`: Auto-map JSON fields to columns
- `ON_ERROR = SKIP_FILE`: Skip files with errors instead of failing the entire load
- `run_query()`: Executes the SQL within dbt
- `log()`: Outputs execution status to dbt logs


---

## 4. Airflow Orchestration

### DAG Configuration

```python
from airflow.decorators import dag
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt_snowflake"
DBT_PROFILES_DIR = "/usr/local/airflow/include/dbt"

@dag(
    schedule="@hourly",  # Run every hour
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["s3", "dbt"],
)
def demo_s3_triggered_dbt():

    # Wait for new data in S3
    wait_for_s3_data = S3KeySensor(
        task_id="wait_for_s3_data",
        aws_conn_id="aws_default",
        bucket_name="your-bucket-name",
        bucket_key="raw-events/{{ macros.ds_format(ds, '%Y-%m-%d', '%Y/%m/%d') }}/*",
        wildcard_match=True,
        poke_interval=3600,
        mode="reschedule",
    )

    # Load raw data from S3 to Snowflake
    load_raw_data = BashOperator(
        task_id="load_raw_data",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run-operation copy_into_raw_events",
        pool="snowflake_pool",
    )

    # Run dbt models and tests
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
        pool="snowflake_pool",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
        pool="snowflake_pool",
    )

    # Pipeline flow
    wait_for_s3_data >> load_raw_data >> dbt_run >> dbt_test

demo_s3_triggered_dbt()
```

### Key Configuration

- **`schedule="@hourly"`**: DAG runs every hour automatically
- **`S3KeySensor`**: Detects new files in S3 with dynamic date partitioning
- **`pool="snowflake_pool"`**: Limits concurrent Snowflake connections (create pool with 5 slots in Airflow UI)
- **`mode="reschedule"`**: Frees up worker slots while waiting for S3 files

---

## 5. dbt Connection Configuration

### profiles.yml

```yaml
dbt_snowflake:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your-account-id
      user: your-username
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: SYSADMIN
      database: YOUR_DATABASE
      warehouse: YOUR_WAREHOUSE
      schema: YOUR_SCHEMA
      threads: 16
```

**Key Parameters**:
- **`account`**: Snowflake account identifier
- **`warehouse`**: Compute cluster for query execution
- **`threads`**: Number of parallel model executions

---

## 6. Data Transformation (Staging Layer)

### Staging Model: `stg_events`

This model parses the raw JSON data and creates a structured table:


```sql
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
```

### Incremental Configuration

**`materialized='incremental'`**: Only process new data on subsequent runs

**How it works**:
1. **First run**: Processes all historical data
2. **Subsequent runs**: Only processes events from the last 3 days
3. **`unique_key='event_id'`**: Prevents duplicates via merge/upsert
4. **3-day lookback**: Handles late-arriving events


## 7. Pipeline Execution Flow


### Step-by-Step Execution

1. **Data Arrival**: Events stream to S3 in gzipped JSON format
2. **Detection**: S3KeySensor polls S3 every hour for new files
3. **Loading**: `COPY INTO` loads raw JSON into Snowflake `RAW_EVENTS` table
4. **Dependencies**: `dbt deps` installs required packages
5. **Transformation**: `dbt run` executes staging and downstream models
6. **Validation**: `dbt test` runs data quality checks
7. **Logging**: Final task logs success/failure status



---

## Summary

This data ingestion pipeline provides:

| Feature | Benefit |
|---------|---------|
| **Event-driven** | Automatically processes new data as it arrives |
| **Scalable** | Handles growing data volumes with incremental processing |
| **Secure** | Uses IAM roles instead of long-lived credentials |
| **Testable** | Validates data quality with automated tests |
| **Observable** | Monitors execution across Airflow, dbt, and Snowflake |
| **Cost-optimized** | Uses concurrency control and incremental models |

The pipeline is production-ready and follows modern data engineering best practices.
