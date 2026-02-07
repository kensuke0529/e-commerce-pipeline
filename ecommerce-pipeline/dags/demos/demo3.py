'''

1. Branching: choose a path based on upstream result
2. Trigger rules: “Run this even if upstream failed/skipped” (super important in real pipelines)

TriggerRule.ALL_DONE: the task runs when all its upstream tasks are finished
TriggerRule.ALL_FAILED: the task runs when all its upstream tasks have failed
TriggerRule.ALL_SKIPPED: the task runs when all its upstream tasks have been skipped
TriggerRule.ONE_FAILED: the task runs when at least one of its upstream tasks has failed
TriggerRule.ONE_SUCCESS: the task runs when at least one of its upstream tasks has succeeded
TriggerRule.ONE_DONE: the task runs when at least one of its upstream tasks is done (in any terminal state)

'''

import random
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)
threshold = 50

@dag(
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["learning"],
)
def demo3():

    @task
    def generate_number() -> int:
        num = random.randint(1, 100)

        # %s is a placeholder for a variable
        log.info("Generated number: %s", num)
        return num

    @task
    def validate_number(num: int) -> None:
        log.info("Validating num=%s against threshold=%s", num, threshold)

        if num < threshold:
            raise AirflowFailException(f"{num} is less than {threshold}")

        log.info("Passed validation")
        # NOTE: return value not needed if you only want validation behavior

    @task.branch
    def decide_path(num: int) -> str:
        # Branch returns the task_id of the next task to run
        return "under_70" if num < 70 else "over_70"

    @task
    def under_70(num: int) -> None:
        log.info("UNDER 70 path. num=%s", num)

    @task
    def over_70(num: int) -> None:
        log.info("OVER 70 path. num=%s", num)

    @task(
        trigger_rule=TriggerRule.ALL_DONE
    )
    def final_task(num: int) -> None:
        log.info("FINAL TASK runs no matter what. num=%s", num)

    # define task dependencies
    num = generate_number()
    branch = decide_path(num)

    u = under_70(num)
    o = over_70(num)
    branch >> [u, o]

    v = validate_number(num)
    [u, o, v] >> final_task(num)

demo3()
