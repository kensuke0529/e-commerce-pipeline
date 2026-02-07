"""

TaskGroup 

"""

import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt_snowflake"
DBT_PROFILES_DIR = "/usr/local/airflow/include/dbt"

@dag(
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["learning", "dbt", "snowflake"],
)
def demo_dbt_snowflake_map_run_test_taskgroup():

    @task
    def list_models() -> list[dict]:
        """Dynamically list all dbt models using `dbt ls`"""
        import subprocess

        subprocess.run(
            ["dbt", "deps", "--profiles-dir", DBT_PROFILES_DIR],
            cwd=DBT_PROJECT_DIR,
            check=True,
            capture_output=True,
            text=True,
        )

        result = subprocess.run(
            [
                "dbt", "ls",
                "--resource-type", "model",
                "--profiles-dir", DBT_PROFILES_DIR,
            ],
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
            check=True,
        )

        params = []
        for line in result.stdout.splitlines():
            line = line.strip()
            if line and " " not in line:
                params.append({"model": line})

        log.info("Found %d models: %s", len(params), [p["model"] for p in params])
        return params

    model_params = list_models()

    # TaskGroup: dbt runs
    with TaskGroup(group_id="dbt_run_group") as dbt_run_group:
        dbt_run = (
            BashOperator
            .partial(
                task_id="dbt_run",
                pool="snowflake_pool",
                env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
                bash_command=(
                    f"cd {DBT_PROJECT_DIR} "
                    "&& dbt run --select {{ params.model }} --profiles-dir " + DBT_PROFILES_DIR
                ),
            )
            .expand(params=model_params)
        )

    # TaskGroup: dbt tests
    with TaskGroup(group_id="dbt_test_group") as dbt_test_group:
        dbt_test = (
            BashOperator
            .partial(
                task_id="dbt_test",
                pool="snowflake_pool",
                env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
                bash_command=(
                    f"cd {DBT_PROJECT_DIR} "
                    "&& dbt test --select {{ params.model }} --profiles-dir " + DBT_PROFILES_DIR
                ),
            )
            .expand(params=model_params)
        )

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def final_report() -> None:
        log.info("Final report: dbt run/test mapping completed.")

    # dependencies between TaskGroups
    model_params >> dbt_run_group >> dbt_test_group >> final_report()

demo_dbt_snowflake_map_run_test_taskgroup()
