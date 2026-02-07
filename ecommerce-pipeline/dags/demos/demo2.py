'''

add retries to the ask_number task
logging

'''

import random
from datetime import timedelta
import logging

from airflow.exceptions import AirflowException
from airflow.decorators import dag, task

# logging.getLogger(__name__): This is a standard Python way to get a logger instance for the current module.
log = logging.getLogger(__name__)

# 1. Define a DAG (the workflow)
@dag(
    schedule=None, 
    catchup=False, 
    tags=["learning"],
)


def demo2():
    # 2. Define Tasks (units of work)
    @task
    def generate_number():
        num = random.randint(1, 100)

        # log.info(...): This writes a message to the Airflow task logs.
        log.info(f'Generated number: {num}')
        return num
    
    @task(
        retries=3,
        retry_delay=timedelta(seconds=3)
    )
    def check_number(num):
        num2 = random.randint(1, 100)
        log.info(f'Generated number2: {num2}')
        if num < num2:
            # AirflowException: A special exception class that tells Airflow: "this task should be marked as FAILED."
            # use AirflowFailException to mark the task as failed immediately
            raise AirflowException(f"{num} is less than {num2}")
        log.info('Passed test')
        return num2
    
    @task
    def print_number(num, num2):
        log.info(f'Number: {num}')
        log.info(f'Number2: {num2}')
    
    # 3. Define dependencies between tasks
    # num -> check_number -> print_number

    num = generate_number()
    num2 = check_number(num)
    print_number(num, num2) 


demo2()
