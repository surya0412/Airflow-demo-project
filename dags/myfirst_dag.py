from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from group_dag.groupedDag import subdagfunction
from airflow.operators.subdag import SubDagOperator

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def some():
    return "Hello this is my dag with python operator"

def some2():
    return "Hello this is my dag with python operator"

def some1():
    return "Hello this is my dag with python operator"

with DAG("mydag", start_date=datetime(2022, 1 ,1),
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    args = {
        'start_date': dag.start_date, 'schedule_interval': dag.schedule_interval, 'catchup': dag.catchup
    }
    T1 = PythonOperator(
        task_id="T1",
        python_callable=some
    )
    T4 = PythonOperator(
        task_id="T4",
        python_callable=some2
    )


    subdag = SubDagOperator(
        task_id="subdag",
        subdag=subdagfunction(dag.dag_id,"subdag",args)
    )

    T1 >> T4 >> subdag
