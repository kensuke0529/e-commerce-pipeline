'''

Airflow DAG syntax (TaskFlow API)
1. Define a DAG (the workflow)
2. Define Tasks (units of work)
3. Define dependencies between tasks

'''

import random
from airflow.decorators import dag, task

# 1. Define a DAG (the workflow)
@dag(
    schedule=None, # how often the DAG should run (ex. @daily, @hourly, @weekly, @monthly)
    catchup=False, # whether to run the DAG for past dates
    tags=["learning"],
)


def demo1():
    # 2. Define Tasks (units of work)
    @task
    def generate_number():
        num = random.randint(1, 100)
        return num
    
    @task
    def print_number(num):
        print(f'Number: {num}')
    
    # 3. Define dependencies between tasks
    # num -> print_number

    num = generate_number()
    print_number(num)


demo1()
