from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from anonymise_dump import Anonymiser

def export_lite_db():
    return "export_lite_db world!"


dag = DAG(
    "export_lite_db",
    description="Simple tutorial DAG",
    schedule_interval="0 12 * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
)

export_lite_db_operator = BashOperator(
    task_id="export_lite_db", bash_command="PGUSER='postgres' PGPASSWORD='password' pg_dump -h localhost -p 5462 lite-api > outfile.sql", dag=dag
)


def anonymise(ds, **kwargs):
    anonymiser = Anonymiser()
    anonymiser.anonymise()


anonymise_operator = PythonOperator(
    task_id="anonymise", python_callable=anonymise, dag=dag
)

export_lite_db_operator >> anonymise_operator