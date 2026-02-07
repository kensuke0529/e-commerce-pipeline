# dbt DAG Flow Explanation

This dbt project transforms raw e-commerce event data from S3 into analytics-ready tables in Snowflake. The pipeline is orchestrated by Apache Airflow and follows a **medallion architecture** pattern with four  layers: **Source → Staging → Intermediate → Marts → Analytics**.


## Airflow Orchestration

### DAG: `demo_s3_triggered_dbt`

![image](../../../images/image-2.png)

### Task Breakdown

1. **`wait_for_s3_data`** (S3KeySensor)
   - Monitors S3 bucket for new event files
   - Bucket: `ecommerce-streaming-data-0001`
   - Path pattern: `raw-events/YYYY/MM/DD/*`
   - Checks every hour, waits up to 3 hours

2. **`load_raw_data`** (BashOperator)
   - Runs dbt macro: `copy_into_raw_events`
   - Executes Snowflake `COPY INTO` command
   - Loads JSON files from S3 stage into `RAW_EVENTS` table

3. **`dbt_deps`** (BashOperator)
   - Installs dbt packages: `dbt_utils`, `dbt_expectations`, `dbt_date`

4. **`dbt_run`** (BashOperator)
   - Executes all dbt models in dependency order
   - Uses Snowflake connection pool to limit concurrency

5. **`dbt_test`** (BashOperator)
   - Runs data quality tests defined in schema.yml files

6. **`logging`** 

## dbt Model Layers

### 1. **Staging Layer** (`models/staging/`)

#### `stg_events.sql`
- **Materialization**: Incremental (merge strategy)
- **Purpose**: Parse raw JSON events into typed columns
- **Incremental Logic**: 
  - Processes new events + 3-day lookback for late arrivals
  - Merge on `event_id` (unique key)
- **Transformations**:
  - Flatten nested JSON fields 
  - Type casting (timestamp, string, float)


### 2. **Intermediate Layer** (`models/intermediate/`)

These models perform aggregations and transformations.

#### `int_customer_purchase_history.sql`
- **Materialization**: Ephemeral (not materialized)
- **Logic**:
  - Filters for `order_fulfilled` events
  - Groups by customer
  - Calculates: `total_orders`, `total_spent`, `last_order_date`

#### `int_session_events.sql`
- **Materialization**: View
- **Logic**:
  - Groups events by `session_id`
  - Calculates: session duration, event count, conversion flag

#### `int_order_items_flattened.sql`
- **Materialization**: View
- **Logic**:
  - Uses Snowflake's `LATERAL FLATTEN` to unnest JSON array
  - One row per product in each order

### 3. **Marts Layer** (`models/marts/`)

#### `dim_customers.sql`
- **Materialization**: Incremental table
- **Incremental Logic**:
  - Merge on `customer_id`

#### `fct_orders.sql`
- **Materialization**: Table
- **Grain**: One row per order

#### `fct_order_items.sql`
- **Materialization**: Table
- **Grain**: One row per product per order



---

### 4. **Analytics Layer** (`models/analytics/`)

Pre-joined, denormalized tables optimized for BI tools (no joins required).

#### `analytics_orders.sql`
- **Materialization**: Table
- **Grain**: One row per order


#### `analytics_order_items.sql`
- **Materialization**: Table
- **Grain**: One row per product per order

#### `analytics_customer_summary.sql`
- **Materialization**: Table
- **Grain**: One row per customer


## Data Flow Summary

### Step-by-Step Transformation

1. **Raw Events** → S3 bucket (JSON files)
2. **COPY INTO** → Snowflake `RAW_EVENTS` table
3. **Staging** → Parse JSON, type cast, incremental merge
4. **Intermediate** → Aggregate sessions, flatten items, calculate customer history
5. **Marts** → Build star schema (dimensions + facts)
6. **Analytics** → Pre-join for BI tools


## Key Features

### 1. **Incremental Processing**
- `stg_events`: Processes only new events with 3-day lookback for late arrivals
- `dim_customers`: Refreshes only customers with new orders

### 2. **Data Quality**
- Schema contracts enforce data types
- dbt tests validate data integrity
- Audit macros for custom checks

### 3. **Performance Optimization**
- Ephemeral models avoid unnecessary materialization
- Incremental models reduce processing time
- Pre-joined analytics tables eliminate runtime joins

### 4. **Separation of Concerns**
- **Marts Layer**: For data engineers (normalized, star schema)
- **Analytics Layer**: For business users (denormalized, no joins)

