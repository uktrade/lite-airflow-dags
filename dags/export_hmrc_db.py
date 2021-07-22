import json
import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from anonymiser import HMRCDBAnonymiser
from boto3.session import Session


class ANON_VARS:
    VCAP_SERVICES = json.loads(os.getenv("VCAP_SERVICES"))
    PAAS_DB = [
        item["credentials"]
        for item in VCAP_SERVICES["postgres"]
        if item["binding_name"] == "anonymise-hmrc-db"
    ]
    PAAS_S3 = [
        item["credentials"]
        for item in VCAP_SERVICES["aws-s3-bucket"]
        if item["binding_name"] == "anonymise-api-s3"
    ]
    LITE_HMRC_URL = PAAS_DB[0]["uri"]
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
        Filename=Variable.get("lite_hmrc_anonymised_sql"),
        Bucket=ANON_VARS.BUCKET_NAME,
        Key="anonymised_lite_hmrc.sql",
    )


def anonymise_hmrc_db(ds, **kwargs):
    anonymiser = HMRCDBAnonymiser()
    anonymiser.anonymise(
        Variable.get("lite_hmrc_dump_sql"), Variable.get("lite_hmrc_anonymised_sql")
    )


with DAG(
    "export_hmrc_db",
    description="Task that anonymises data in LITE-HMRC database",
    schedule_interval="0 1 * * *",
    start_date=datetime(2021, 7, 16),
    catchup=False,
) as dag:

    dump_db_operator = BashOperator(
        task_id="dump_lite_hmrc_db",
        bash_command=f"pg_dump {ANON_VARS.LITE_HMRC_URL} >"
        f"{Variable.get('lite_hmrc_dump_sql')}",
        dag=dag,
    )

    anonymise_operator = PythonOperator(
        task_id="anonymise_hmrc_db", python_callable=anonymise_hmrc_db, dag=dag
    )

    push_to_s3_operator = PythonOperator(
        task_id="push_to_s3",
        python_callable=push_to_s3,
        dag=dag,
    )

    delete_dumps_operator = BashOperator(
        task_id="delete_dumps",
        bash_command=f"rm {Variable.get('lite_hmrc_dump_sql')} {Variable.get('lite_hmrc_anonymised_sql')}",
        dag=dag,
    )

    (
        dump_db_operator
        >> anonymise_operator
        >> push_to_s3_operator
        >> delete_dumps_operator
    )
