from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from anonymise_dump import Anonymiser


def anonymise(**kwargs):
    print("======> Anonymising the database ....")
    # anonymiser = Anonymiser()
    # anonymiser.anonymise()



with DAG(
    "export_lite_db",
    description="Simple tutorial DAG",
    schedule_interval="0 12 * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
) as dag:

    dump_db_operator = BashOperator(
        task_id="dump_db",
        bash_command="PGUSER='postgres' PGPASSWORD='password' pg_dump -h localhost -p 5462 lite-api > outfile.sql",
        dag=dag,
    )

    anonymise_operator = PythonOperator(
        task_id="anonymise", python_callable=anonymise, dag=dag
    )

    push_to_s3_operator = BashOperator(
        task_id="push_to_s3",
        bash_command="echo push_to_s3_operator placeholder",
        dag=dag,
    )

    delete_dumps_operator = BashOperator(
        task_id="delete_dumps", bash_command="echo delete_dumps", dag=dag
    )

    # (
    #     dump_db_operator
    #     >> anonymise_operator
    #     >> push_to_s3_operator
    #     >> delete_dumps_operator
    # )

    anonymise_operator
