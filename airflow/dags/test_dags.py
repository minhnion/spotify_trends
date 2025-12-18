import random
from pendulum import datetime
from datetime import timedelta

from airflow.decorators import dag, task

@dag(
    start_date=datetime(2025, 1, 1, tz="UTC"),
    schedule_interval=timedelta(seconds=30),
    catchup=False,
    tags=['example'],
)
def test_operator():
    @task()
    def get_random_number():
        n1 = random.choice(range(100))
        n2 = random.choice(range(100))

        return [n1, n2]
    @task()
    def add_number(numbs: list):
        return sum(numbs)

    @task.virtualenv(
        task_id="virtualenv_python", requirements=["numpy"], system_site_packages=False
    )
    def mul_number(numbs: list):
        import numpy
        return int(numpy.prod(numpy.array(numbs)))
    
    @task()
    def display(add_res, mul_res):
        return (add_res, mul_res)
    task1 = get_random_number()
    task2 = add_number(task1)
    task3 = mul_number(task1)
    display(task2, task3)
test_dag = test_operator()
