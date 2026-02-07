
"""
Dynamic Task Mapping + Pools 
https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html

Idea:
- Build a list of "job specs" (one per table/process)
- Fan out (map) one task instance per job spec
- Limit concurrency with a pool (warehouse_pool)
- Always run a final audit/report task
"""

import logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

@dag(
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["learning"],
)
def demo4_mapping_jobspecs():

    # Generate "what should we run?" dynamically
    @task
    def build_job_specs() -> list[dict]:
        jobs = [
            {
                "name": "dim_customers",
                "table": "customers",
                "mode": "full_refresh",
                "sql": "SELECT * FROM raw.customers",  # demo SQL
            },
            {
                "name": "fct_orders",
                "table": "orders",
                "mode": "incremental",
                "sql": "SELECT * FROM raw.orders WHERE order_date >= '{{ ds }}'",  # demo SQL
            },
            {
                "name": "fct_events",
                "table": "events",
                "mode": "dedupe",
                "sql": "SELECT * FROM raw.events WHERE event_date = '{{ ds }}'",  # demo SQL
            },
        ]
        log.info("Built %s job specs", len(jobs))
        return jobs

    # One mapped task = one job spec
    # Pool limits how many can run at once (protect your warehouse)
    @task(pool="warehouse_pool")
    def run_transform(job: dict) -> dict:
        # same wrapper, different behavior via config.

        name = job["name"]
        table = job["table"]
        mode = job["mode"]
        sql = job["sql"]

        log.info("Running job=%s table=%s mode=%s", name, table, mode)
        log.info("SQL (demo): %s", sql)

        # REAL IMPLEMENTATION OPTIONS:
        # - BigQuery: use BigQueryInsertJobOperator or google-cloud-bigquery client
        # - Snowflake: SnowflakeOperator or snowflake-connector-python

        return {
            "name": name,
            "table": table,
            "mode": mode,
            "status": "success",
            "rows_loaded": 1234,  # placeholder
        }

    # Reduce / final reporting step
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def final_audit(results) -> None:
        results_list = list(results)  
        log.info("FINAL AUDIT: got %s results", len(results_list))

        succeeded = [r for r in results_list if r.get("status") == "success"]
        failed = [r for r in results_list if r.get("status") != "success"]

        log.info("Succeeded=%s Failed=%s", len(succeeded), len(failed))

    jobs = build_job_specs()

    # .expand() fan-out
    results = run_transform.expand(job=jobs)  
    final_audit(results)

demo4_mapping_jobspecs()
