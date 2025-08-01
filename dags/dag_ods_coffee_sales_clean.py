"""
DAG: Очистка и загрузка данных в ODS
Источник: stg.sales_raw
"""
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

# Константы
DAG_ID = "clean_coffee_ods"
POSTGRES_CONN_ID = "postgres"
SOURCE_SCHEMA = "stg"
SOURCE_TABLE = "sales_raw"
TARGET_SCHEMA = "ods"
TARGET_TABLE = "sales_clean"


def get_last_load_timestamp(**context) -> datetime:
    """
    Получает время последней загрузки в ODS.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    result = hook.get_first(
        f"""
        SELECT COALESCE(MAX(load_dttm), '1970-01-01'::TIMESTAMP)
        FROM {TARGET_SCHEMA}.{TARGET_TABLE}
        """
    )
    return result[0]


def extract_raw_data(**context):
    """
    Извлекает новые данные из STG.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    last_loaded = context["task_instance"].xcom_pull(task_ids="get_last_load_timestamp")
    query = f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} WHERE load_dttm > '{last_loaded}'"

    df = pd.read_sql(query, con=engine)
    if df.empty:
        context["task_instance"].xcom_push(key="no_new_data", value=True)
    
    df["load_dttm"] = df["load_dttm"].astype(str)
    df = df.where(pd.notnull(df), None)

    return df.to_dict("records")


def transform_data(**context):
    """
    Очищает и преобразует данные.
    """
    data = context["task_instance"].xcom_pull(task_ids="extract_raw_data")
    if context["task_instance"].xcom_pull(task_ids="extract_raw_data", key="no_new_data"):
        print("Нет новых данных для очистки.")
        return None

    df = pd.DataFrame(data)

    df = df.replace(["NULL", "nan"], None)
    df = df.dropna(subset=["date", "datetime", "money", "coffee_name", "cash_type"], how="all")

    df["money"] = df["money"].astype(str).str.extract(r"(\d+\.?\d*)").astype(float).round(2)
    df["money"].fillna(0.00, inplace=True)

    df = df.drop_duplicates()

    ods_df = df[["date", "hour_of_day", "cash_type", "card", "money", "coffee_name", "Time_of_Day", "Weekdaysort", "Monthsort"]].rename(
        columns={
            "date": "sale_date",
            "Time_of_Day": "time_of_day",
            "Weekdaysort": "weekdaysort",
            "Monthsort": "monthsort",
        }
    )
    ods_df["source"] = "coffee_sales.xlsx"
    ods_df["load_dttm"] = datetime.now()

    def serialize(obj):
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        if isinstance(obj, (np.datetime64, pd.Timestamp)):
            return obj.isoformat()
        if pd.isna(obj):
            return None
        return obj

    records = ods_df.to_dict("records")
    clean_records = [
        {k: serialize(v) for k, v in record.items()} for record in records
    ]

    return clean_records


def load_to_ods(**context):
    """
    Загружает очищенные данные в ODS.
    """
    data = context["task_instance"].xcom_pull(task_ids="transform_data")
    if data is None:
        return

    df = pd.DataFrame(data)
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        TARGET_TABLE,
        con=engine,
        schema=TARGET_SCHEMA,
        if_exists="append",
        index=False,
    )
    print(f"Очищено и загружено {len(df)} строк в {TARGET_SCHEMA}.{TARGET_TABLE}")


default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id=DAG_ID,
    description="ETL: stg → ods (очистка, dedup, инкремент)",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "ods"],
    default_args=default_args,
    max_active_runs=1,
)

get_last_task = PythonOperator(
    task_id="get_last_load_timestamp",
    python_callable=get_last_load_timestamp,
    dag=dag,
)

extract_task = PythonOperator(
    task_id="extract_raw_data",
    python_callable=extract_raw_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_to_ods",
    python_callable=load_to_ods,
    dag=dag,
)

get_last_task >> extract_task >> transform_task >> load_task