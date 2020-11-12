import sqlalchemy
from airflow import DAG
from airflow.hooks.oracle_hook import OracleHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


class OracleSqlAlchemyHook(OracleHook):
    def get_conn(self):
        conn_id = getattr(self, self.conn_name_attr)
        connection_uri = self.get_connection(conn_id).get_uri()
        engine = sqlalchemy.create_engine(connection_uri)
        return engine.connect()


def query_to_csv(**kwargs):
    oracle = OracleSqlAlchemyHook()
    df = oracle.get_pandas_df("select 1 from DUAL")
    df.to_csv("/tmp/df.csv")


with DAG(
    "spire2csv",
    description="Simple tutorial DAG",
    schedule_interval="0 12 * * *",
    start_date=days_ago(2),
    catchup=False,
) as spire2csv:
    task = PythonOperator(task_id="query_to_csv", python_callable=query_to_csv,)
