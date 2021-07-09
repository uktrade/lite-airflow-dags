from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from anonymise_dump import Anonymiser
from airflow.models import Variable

def anonymise(ds, **kwargs):
    anonymiser = Anonymiser()
    anonymiser.anonymise(Variable.get('sql_dumpfile'), Variable.get('sql_anonfile'))


with DAG(
    "export_lite_db",
    description="Simple tutorial DAG",
    schedule_interval="0 12 * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
) as dag:

    dump_db_operator = BashOperator(
        task_id="dump_db",
        bash_command=f"PGUSER={Variable.get('lite_db_user')} "
                     f"PGPASSWORD={Variable.get('lite_db_password')} "
                     f"pg_dump -h {Variable.get('lite_db_host')} "
                     f"-p {Variable.get('lite_db_port')} "
                     f"{Variable.get('lite_db_database')} > "
                     f"{Variable.get('sql_dumpfile')}",
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

    (
        dump_db_operator
        >> anonymise_operator
        >> push_to_s3_operator
        >> delete_dumps_operator
    )
