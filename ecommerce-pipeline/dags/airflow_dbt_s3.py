"""

S3-triggered dbt pipeline:
1. Wait for new data files in S3 using S3KeySensor
2. Once detected, run dbt deps to install dependencies
3. Run dbt models to transform the data
4. Run dbt tests to validate the transformations

"""

from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
import logging

log = logging.getLogger(__name__)

DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt_snowflake"
DBT_PROFILES_DIR = "/usr/local/airflow/include/dbt"

@dag(
    schedule="@hourly",   # change this config to schedule
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=[ "s3", "dbt"],
)
def demo_s3_triggered_dbt():

    # Wait for new data files in S3
    wait_for_s3_data = S3KeySensor(
        task_id="wait_for_s3_data",
        aws_conn_id="aws_default",
        bucket_name="ecommerce-streaming-data-0001",
        
        # S3 structure is partitioned by date/hour: raw-events/YYYY/MM/DD/HH/file.gz
        bucket_key="raw-events/{{ macros.ds_format(ds, '%Y-%m-%d', '%Y/%m/%d') }}/*",
        wildcard_match=True,
        
        poke_interval=60 * 60,      # Check every hour
        timeout=60 * 60 * 3,       # Wait up to 3 hours
        mode="reschedule",   
    )

    # Load raw data from S3 to Snowflake
    load_raw_data = BashOperator(
        task_id="load_raw_data",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run-operation copy_into_raw_events --profiles-dir {DBT_PROFILES_DIR}",
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
        pool="snowflake_pool",
    )

    # Install dbt dependencies
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
    )

    # Run dbt models to transform the data
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR}",
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
        pool="snowflake_pool",  # Limit concurrent Snowflake connections
    )

    # Run dbt tests to validate transformations
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}",
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
        pool="snowflake_pool",
    )

    # Final report
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def logging():
        log.info("S3-triggered dbt pipeline completed successfully!")
        log.info("Data detected in S3, transformed by dbt, and validated by tests.")

    # Define the pipeline flow
    wait_for_s3_data >> load_raw_data >> dbt_deps >> dbt_run >> dbt_test >> logging()

demo_s3_triggered_dbt()
