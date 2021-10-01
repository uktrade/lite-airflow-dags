import json
import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from anonymiser import DBAnonymiser
from boto3.session import Session


class ANON_VARS:
    VCAP_SERVICES = json.loads(os.getenv("VCAP_SERVICES"))
    PAAS_DB = [
        item["credentials"]
        for item in VCAP_SERVICES["postgres"]
        if item["binding_name"] == "anonymise-api-db"
    ]
    PAAS_S3 = [
        item["credentials"]
        for item in VCAP_SERVICES["aws-s3-bucket"]
        if item["binding_name"] == "anonymise-api-s3"
    ]
    LITE_DB_URL = PAAS_DB[0]["uri"]
    BUCKET_NAME = PAAS_S3[0]["bucket_name"]
    AWS_ACCESS_KEY_ID = PAAS_S3[0]["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = PAAS_S3[0]["aws_secret_access_key"]
    AWS_REGION = PAAS_S3[0]["aws_region"]


def push_to_s3(ds, **kwargs):
    session = Session(
        aws_access_key_id=ANON_VARS.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=ANON_VARS.AWS_SECRET_ACCESS_KEY,
        region_name=ANON_VARS.AWS_REGION,
    )
    s3 = session.resource("s3")
    s3.meta.client.upload_file(
        Filename=Variable.get("sql_anonfile"),
        Bucket=ANON_VARS.BUCKET_NAME,
        Key="anonymised.sql",
    )


def anonymise(ds, **kwargs):
    anonymiser = DBAnonymiser()
    anonymiser.anonymise(Variable.get("sql_dumpfile"), Variable.get("sql_anonfile"))


with DAG(
    "export_lite_db",
    description="Simple tutorial DAG",
    schedule_interval="0 1 * * *",
    start_date=datetime(2021, 7, 16),
    catchup=False,
) as dag:

    dump_db_operator = BashOperator(
        task_id="dump_db",
        bash_command=f"pg_dump {ANON_VARS.LITE_DB_URL} >"
        f"{Variable.get('sql_dumpfile')}",
        dag=dag,
    )

    anonymise_operator = PythonOperator(
        task_id="anonymise", python_callable=anonymise, dag=dag
    )

    push_to_s3_operator = PythonOperator(
        task_id="push_to_s3", python_callable=push_to_s3, dag=dag,
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
