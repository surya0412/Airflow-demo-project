from airflow import DAG

from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

def fun3():
    return "This is fun3 (grouped dag)"

def fun4():
    return "This is fun4 (grouped dag)"

def subdagfunction(parent_dag_id, child_dag_id, args):

    with DAG (f"{parent_dag_id}.{child_dag_id}",
                    start_date=args["start_date"],
                    schedule_interval=args["schedule_interval"],
                    catchup=args["catchup"]) as dag:
        t3 = PythonOperator(
            task_id= "t3",
            python_callable=fun3
        )
        t4 = PythonOperator(
            task_id= "t4",
            python_callable=fun4
        )
        return dag

