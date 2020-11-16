import sqlalchemy
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.oracle_hook import OracleHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import s3fs


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


def query_to_csv(**kwargs):
    query = kwargs["query"]
    parameters = kwargs.get("parameters", {})
    dest_file = kwargs["dest_file"]

    oracle = OracleSqlAlchemyHook()
    df = oracle.get_pandas_df(query, parameters=parameters)
    s3_conn = BaseHook.get_connection("s3_default")
    access_key = s3_conn.extra_dejson["aws_access_key_id"]
    secret = s3_conn.extra_dejson["aws_secret_access_key"]
    bucket_name = s3_conn.extra_dejson["bucket_name"]
    s3 = s3fs.S3FileSystem(anon=False, key=access_key, secret=secret)

    if dest_file:
        with s3.open(
            f"{bucket_name}/{kwargs['ds']}/{kwargs['task_instance_key_str']}/{dest_file}.csv",
            "wb",
        ) as csvfile:
            df.to_csv(
                csvfile, index=False,
                mode="wb"
            )


xview_export_licences = """
SELECT
    ID,
    ELA_GRP_ID,
    LICENCE_REF,
    LICENCE_STATUS,
    START_DATE,
    END_DATE,
    NULL, -- (XML_DATA).getClobVal() XML_DATA,
    XP.GET(XML_DATA, '/*/OGL/OGL_TYPE') OGL_TYPE,
    XP.GET(XML_DATA, '/*/OGL/OGL_TITLE') OGL_TITLE
FROM SPIREMGR.EXPORT_LICENCES 
ORDER BY ID {LIMIT}
"""

export_licence_details = """
SELECT
    ID,
    L_ID,
    ELA_ID,
    ELA_GRP_ID,
    ELA_DETAIL_ID,
    N_ID,
    START_DATE,
    END_DATE,
    NULL, -- (XML_DATA).getClobVal() LICENCE_DETAIL_XML,
    LICENCE_TYPE,
    LICENCE_SUB_TYPE,
    OGL_ID,
    DI_ID,
    EXPIRY_DATE,
    LICENCE_REF,
    LEGACY_FLAG,
    CUSTOMS_EX_PROCEDURE,
    CREATED_BY_WUA_ID,
    UREF_VALUE,
    COMMENCEMENT_DATE,
    LITE_APP
FROM SPIREMGR.EXPORT_LICENCE_DETAILS
ORDER BY ID {LIMIT}
"""

queries = {
    "xview_export_licences": xview_export_licences,
    "export_licence_details": export_licence_details,
}

with DAG(
    "spire2csv",
    description="Simple tutorial DAG",
    schedule_interval="0 12 * * *",
    start_date=days_ago(2),
    catchup=False,
) as spire2csv:
    connectivity_check = PythonOperator(
        task_id="connectivity_check",
        python_callable=query_to_csv,
        provide_context=True,
        op_kwargs={"query": "select 1 from DUAL", "dest_file": None},
    )

    for table, sql in queries.items():
        task = PythonOperator(
            task_id=f"query_{table}_to_csv",
            provide_context=True,
            python_callable=query_to_csv,
            op_kwargs={
                "query": sql.format(LIMIT="FETCH FIRST 100 ROWS ONLY"),
                "dest_file": f"{table}.csv",
            },
        )
        connectivity_check >> task
