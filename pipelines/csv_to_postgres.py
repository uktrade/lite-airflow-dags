import csv
import sqlalchemy
import pandas as pd

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


test_data = [
    {"id": 1, "name": "airflow"},
    {"id": 2, "name": "postgres"},
    {"id": 3, "name": "oracle"},
]


class PostgresSqlAlchemyHook(PostgresHook):
    def get_conn(self):
        connection_uri = self.get_connection("local_postgres_data").get_uri()
        engine = sqlalchemy.create_engine(connection_uri)
        return engine


def csv_to_postgres(**kwargs):
    with open("/tmp/input_data.csv", "w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["id", "name"])
        writer.writeheader()
        for data in test_data:
            writer.writerow(data)

    pg = PostgresSqlAlchemyHook()
    df = pd.read_csv("/tmp/input_data.csv")
    df.to_sql("example", pg, if_exists="replace")


with DAG(
    "csv2postgres",
    description="Simple tutorial DAG",
    schedule_interval="0 12 * * *",
    start_date=days_ago(2),
    catchup=False,
) as csv2postgres:
    task = PythonOperator(
        task_id="csv_to_db",
        python_callable=csv_to_postgres,
    )
