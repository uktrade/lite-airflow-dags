import logging
import os
import sqlalchemy
import pandas as pd

from contextlib import closing
from datetime import date
from environs import Env
from s3fs.core import S3FileSystem

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = os.path.join(BASE_DIR, ".env")
if os.path.exists(ENV_FILE):
    Env.read_env(ENV_FILE)

env = Env()
FILES_TO_SKIP = env.list("FILES_TO_SKIP", [])

logger = logging.getLogger("airflow.task")


class PostgresSqlAlchemyHook(PostgresHook):
    def get_conn(self):
        connection_uri = self.get_connection("spire_local").get_uri()
        engine = sqlalchemy.create_engine(connection_uri)
        return engine.connect()


def get_objects_by_date(s3_client, bucket_name, current_date=None):
    if not current_date:
        current_date = date.today().isoformat()

    keys = []

    for (base_path, _, files) in s3_client.walk(
        os.path.join(bucket_name, current_date)
    ):
        # some of the files have extension repeated (.csv.csv), filter them
        valid_files = [
            os.path.join(base_path, f) for f in files if len(f.split(".")) == 2
        ]

        keys.extend(valid_files)

    logger.info(
        f"Fetched csv files from {os.path.join(bucket_name, current_date)}, {len(keys)} files found"
    )

    return keys


def csv_to_postgres(**kwargs):
    s3_conn = BaseHook.get_connection("s3_default")
    access_key = s3_conn.extra_dejson["aws_access_key_id"]
    secret = s3_conn.extra_dejson["aws_secret_access_key"]
    bucket_name = s3_conn.extra_dejson["bucket_name"]
    s3 = S3FileSystem(anon=False, key=access_key, secret=secret)

    files_to_load = get_objects_by_date(s3, bucket_name)

    with closing(PostgresSqlAlchemyHook().get_conn()) as pg:
        pg.execute("set session_replication_role to 'replica';")
        for index, file_key in enumerate(files_to_load):
            filename = file_key.split("/")[-1]
            table_name = filename.split(".")[0]

            if filename in FILES_TO_SKIP:
                continue

            with s3.open(file_key, "rb") as fp:
                logger.info(f"[{index + 1}] Reading contents of file: {filename} ...")
                df = pd.read_csv(fp)
                logger.info(
                    f"[{index + 1}] Loading file contents into table: {table_name} ..."
                )
                df.to_sql(table_name, pg, if_exists="replace")

        pg.execute("set session_replication_role to 'origin';")


with DAG(
    "csv2postgres",
    description="Read CSV files from S3 and load into Postgres db",
    schedule_interval="0 12 * * *",
    start_date=days_ago(2),
    catchup=False,
) as csv2postgres:
    task = PythonOperator(
        task_id="csv_to_db",
        python_callable=csv_to_postgres,
    )
