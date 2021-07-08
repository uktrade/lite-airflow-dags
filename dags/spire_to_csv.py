from contextlib import closing
from io import TextIOWrapper
from pathlib import Path

import pandas as pd
import s3fs
import sqlalchemy
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.oracle_hook import OracleHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import S3KeySensor
from airflow.utils.dates import days_ago
from pandas import DataFrame

s3_conn = BaseHook.get_connection("aws_default")


class OracleSqlAlchemyHook(OracleHook):
    """
    Override default oracle hook's get_conn
    to use SQLAlchemy instead. This is to allow the same format connection
    string as defined in environment variables to work everywhere.

    The original class expects a cx_Oracle compatible connection string, not
    a sqlalchemy one.
    """

    def get_conn(self):
        conn_id = getattr(self, self.conn_name_attr)
        connection_uri = self.get_connection(conn_id).get_uri()
        engine = sqlalchemy.create_engine(connection_uri)
        return engine.connect()

    def get_pandas_df(self, sql, parameters=None, chunksize=None):
        """
        Executes the sql and returns a pandas dataframe

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        import pandas.io.sql as psql

        with closing(self.get_conn()) as conn:
            return psql.read_sql(sql, con=conn, params=parameters, chunksize=chunksize)


def query_to_csv(**kwargs):
    query = kwargs["query"]
    parameters = kwargs.get("parameters", {})
    dest_file = kwargs["dest_file"]
    chunksize = 5000
    oracle = OracleSqlAlchemyHook()
    df = oracle.get_pandas_df(query, parameters=parameters, chunksize=chunksize)
    access_key = s3_conn.login
    secret = s3_conn.password
    bucket_name = s3_conn.extra_dejson["bucket_name"]
    s3 = s3fs.S3FileSystem(anon=False, key=access_key, secret=secret)
    logger = kwargs["ti"].log
    logger.info(f"Running {kwargs['task_instance_key_str']} for {kwargs['ds']}")

    if dest_file:
        path = f"{bucket_name}/{kwargs['ds']}/{kwargs['task_instance_key_str']}/{dest_file}"
        with s3.open(
            path,
            "wb",
        ) as csvfile:
            first_chunk = True
            tcsv = TextIOWrapper(csvfile)
            chunk: DataFrame
            for idx, chunk in enumerate(df):
                logger.info(f"Processing chunk {idx} of {dest_file}")
                chunk.to_csv(tcsv, index=False, chunksize=chunksize, header=first_chunk)
                first_chunk = False
            tcsv.close()
        return f"s3://{path}"


def get_query_by_filename(filename):
    file_dir = Path(__file__).resolve().parent
    return (file_dir / Path("./queries") / Path(filename)).read_text()


xview_export_licences = get_query_by_filename("xview_export_licences.sql")
export_licence_details = get_query_by_filename("export_licence_details.sql")
ela_case_details = get_query_by_filename("ela_case_details.sql")

queries = {
    "xview_export_licences": {"sql": xview_export_licences, "LIMIT": ""},
    "export_licence_details": {"sql": export_licence_details, "LIMIT": ""},
    "ela_case_details": {"sql": ela_case_details, "LIMIT": "FETCH FIRST 50 ROWS ONLY"},
}

with DAG(
    "spire2csv",
    description="Upload SPIRE Tables to S3",
    schedule_interval="0 12 * * *",
    start_date=days_ago(2),
    catchup=False,
    concurrency=3,
) as spire2csv:
    connectivity_check = PythonOperator(
        task_id="connectivity_check",
        do_xcom_push=True,
        python_callable=query_to_csv,
        provide_context=True,
        op_kwargs={"query": "select 1 from DUAL", "dest_file": None},
    )

    for table, query in queries.items():
        task = PythonOperator(
            task_id=f"query_{table}_to_csv",
            task_concurrency=1,
            provide_context=True,
            python_callable=query_to_csv,
            op_kwargs={
                "query": query["sql"].format(LIMIT=query["LIMIT"]),
                "dest_file": f"{table}.csv",
            },
        )
        connectivity_check >> task


class PostgresSqlAlchemyHook(PostgresHook):
    def get_conn(self):
        connection_uri = self.get_connection("spire_local").get_uri()
        engine = sqlalchemy.create_engine(connection_uri, paramstyle="format")
        return engine.connect()


def csv_to_postgres(**kwargs):
    access_key = s3_conn.login
    secret = s3_conn.password
    s3 = s3fs.S3FileSystem(anon=False, key=access_key, secret=secret)
    logger = kwargs["ti"].log
    file_key = kwargs["file_key"]
    table_name = kwargs["table_name"]

    with closing(PostgresSqlAlchemyHook().get_conn()) as pg:
        pg.execute("set session_replication_role to 'replica';")

        with s3.open(file_key, "rb") as fp:
            logger.info(f"Reading contents of file: {file_key} ...")
            df = pd.read_csv(fp)
            logger.info(f"Loading file contents into table: {table_name} ...")
            df.to_sql(table_name, pg, if_exists="replace", method="multi")

        pg.execute("set session_replication_role to 'origin';")


with DAG(
    "csv2postgres",
    description="Read CSV files from S3 and load into Postgres db",
    schedule_interval="0 12 * * *",
    start_date=days_ago(2),
    catchup=False,
) as csv2postgres:
    bucket_name = s3_conn.extra_dejson["bucket_name"]
    file_key = "s3://{{params.bucket_name}}/{{ds}}/spire2csv__query_{{params.table}}_to_csv__{{ds_nodash}}/{{params.table}}.csv"
    for table, query in queries.items():
        check_for_files = S3KeySensor(
            task_id=f"check_s3_for_{table}_file",
            bucket_key=file_key,
            poke_interval=60,
            params={"table": table, "bucket_name": bucket_name},
            timeout=60 * 60 * 12,  # timeout after 12 hours of waiting for the file
        )

        csv2postgres_task = PythonOperator(
            task_id=f"{table}_csv_to_db",
            python_callable=csv_to_postgres,
            provide_context=True,
            params={"table": table, "bucket_name": bucket_name},
            op_kwargs={"file_key": file_key, "table_name": table},
        )
        check_for_files >> csv2postgres_task
