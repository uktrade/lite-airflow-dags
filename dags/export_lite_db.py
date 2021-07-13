from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from anonymise_dump import DBAnonymiser
from boto3.session import Session


def push_to_s3(ds, **kwargs):
    session = Session(
        aws_access_key_id=Variable.get("aws_access_key_id"),
        aws_secret_access_key=Variable.get("aws_secret_access_key"),
        region_name=Variable.get("aws_region"),
    )
    s3 = session.resource("s3")
    s3.meta.client.upload_file(
        Filename=Variable.get("sql_anonfile"),
        Bucket=Variable.get("bucket_name"),
        Key="anonymised.sql",
    )


def anonymise(ds, **kwargs):
    anonymiser = DBAnonymiser()
    anonymiser.anonymise(Variable.get("sql_dumpfile"), Variable.get("sql_anonfile"))


with DAG(
    "export_lite_db",
    description="Simple tutorial DAG",
    schedule_interval="0 12 * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
) as dag:

    dump_db_operator = BashOperator(
        task_id="dump_db",
        bash_command=f"pg_dump {Variable.get('lite_db_url')} >"
        f"{Variable.get('sql_dumpfile')}",
        dag=dag,
    )

    anonymise_operator = PythonOperator(
        task_id="anonymise", python_callable=anonymise, dag=dag
    )

    push_to_s3_operator = PythonOperator(
        task_id="push_to_s3",
        python_callable=push_to_s3,
        dag=dag,
    )

    delete_dumps_operator = BashOperator(
        task_id="delete_dumps",
        bash_command=f"rm {Variable.get('sql_dumpfile')} {Variable.get('sql_anonfile')}",
        dag=dag,
    )

    (
        dump_db_operator
        >> anonymise_operator
        >> push_to_s3_operator
        >> delete_dumps_operator
    )
