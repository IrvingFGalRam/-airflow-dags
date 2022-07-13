#from datetime import datetime
#from airflow import DAG
#from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.python_operator import PythonOperator

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator


def ingest_data(
    hook = PostgresHook(postgres_conn_id="ml_conn")
    

with DAG(
    "db_ingestion", start_date=days_ago(1), schedule_interval="@once"
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")
    validate = DummyOperator(task_id="validate")
    prepare = PostgresOperator(
        task_id="prepare",
        postgres_conn_id="ml_conn",
        sql=f"""
            CREATE TABLE IF NOT EXISTS monthly_charts_data (
                month VARCHAR(10),
                position INTEGER,
                artist VARCHAR(100),
                song VARCHAR(100),
                indicative_revenue NUMERIC,
                us INTEGER,
                uk INTEGER,
                de INTEGER,
                fr INTEGER,
                ca INTEGER,
                au INTEGER
            )
        """,
    )
    load = DummyOperator(task_id="load")
    end_workflow = DummyOperator(task_id="end_workflow")

    start_workflow >> validate >> prepare >> load >> end_workflow