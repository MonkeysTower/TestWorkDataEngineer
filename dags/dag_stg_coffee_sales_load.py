"""
DAG: Разовая загрузка Excel-файла в STG
"""
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

# Константы
DAG_ID = "load_coffee_raw_once"
DATA_PATH = "/opt/airflow/data/coffee_sales.xlsx"
POSTGRES_CONN_ID = "postgres"
TARGET_SCHEMA = "stg"
TARGET_TABLE = "sales_raw"


def check_file_exists(**context):
    """
    Проверяет, существует ли файл.
    """
    import os

    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"Файл не найден: {DATA_PATH}")
    print(f"Файл найден: {DATA_PATH}")


def load_raw_data(**context):
    """
    Загружает Excel-файл в stg.sales_raw.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_excel(DATA_PATH, dtype=str)
    df["load_dttm"] = datetime.now()
    df["source"] = "coffee_sales.xlsx"

    df.to_sql(
        TARGET_TABLE,
        con=engine,
        schema=TARGET_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
    )
    print(f"Загружено {len(df)} строк в {TARGET_SCHEMA}.{TARGET_TABLE}")


default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id=DAG_ID,
    description="Разовая загрузка из Excel в stg",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["setup", "stg"],
    default_args=default_args,
)

check_file_task = PythonOperator(
    task_id="check_file_exists",
    python_callable=check_file_exists,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_raw_from_excel",
    python_callable=load_raw_data,
    dag=dag,
)

check_file_task >> load_task