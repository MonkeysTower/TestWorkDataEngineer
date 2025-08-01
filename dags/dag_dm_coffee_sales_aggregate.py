"""
DAG: Построение аналитической витрины dm.daily_sales
Источник: ods.sales_clean
"""
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

# Константы
DAG_ID = "build_coffee_dm"
POSTGRES_CONN_ID = "postgres"
TARGET_SCHEMA = "dm"
TARGET_TABLE = "daily_sales"


def extract_last_processed_date(**context) -> datetime:
    """
    Получает последнюю дату обработки из витрины.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    last_date = hook.get_first(
        f"""
        SELECT COALESCE(MAX(load_dttm), '1970-01-01'::TIMESTAMP)
        FROM {TARGET_SCHEMA}.{TARGET_TABLE}
        """
    )[0]
    return last_date


def extract_sales_data(**context):
    """
    Извлекает новые данные из ods.sales_clean.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # Получаем дату из предыдущего шага
    last_date = context["task_instance"].xcom_pull(task_ids="get_last_processed_date")

    query = f"""
    WITH sales_by_hour AS (
        SELECT
            sale_date::DATE AS sale_date,
            hour_of_day AS hour,
            COUNT(*) AS cnt
        FROM ods.sales_clean
        WHERE load_dttm > '{last_date}'
        GROUP BY sale_date, hour
    ),
    top_hour_per_day AS (
        SELECT
            sale_date,
            hour,
            ROW_NUMBER() OVER (PARTITION BY sale_date ORDER BY cnt DESC, hour ASC) AS rn
        FROM sales_by_hour
    )
    SELECT
        sc.sale_date::DATE AS report_date,
        SUM(sc.money) AS revenue,
        ROUND(AVG(sc.money), 2) AS avg_check,
        COUNT(*) AS units_sold,
        th.hour AS top_hour
    FROM ods.sales_clean sc
    LEFT JOIN top_hour_per_day th
        ON sc.sale_date::DATE = th.sale_date AND th.rn = 1
    WHERE sc.load_dttm > '{last_date}'
    GROUP BY sc.sale_date::DATE, th.hour;
    """

    df = pd.read_sql(query, con=engine)
    if df.empty:
        context["task_instance"].xcom_push(key="no_new_data", value=True)
    return df.to_dict("records")


def load_daily_sales(**context):
    """
    Загружает агрегированные данные в dm.daily_sales.
    """
    data = context["task_instance"].xcom_pull(task_ids="extract_sales_data")
    if context["task_instance"].xcom_pull(task_ids="extract_sales_data", key="no_new_data"):
        print("Нет новых данных для витрины.")
        return

    df = pd.DataFrame(data)
    df["load_dttm"] = datetime.now()

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        TARGET_TABLE,
        con=engine,
        schema=TARGET_SCHEMA,
        if_exists="append",
        index=False,
    )
    print(f"Построено {len(df)} строк витрины {TARGET_SCHEMA}.{TARGET_TABLE}")


default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id=DAG_ID,
    description="ETL: ods → dm (агрегация по дням)",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dm", "analytics"],
    default_args=default_args,
    max_active_runs=1,
)

get_last_date_task = PythonOperator(
    task_id="get_last_processed_date",
    python_callable=extract_last_processed_date,
    dag=dag,
)

extract_task = PythonOperator(
    task_id="extract_sales_data",
    python_callable=extract_sales_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_daily_sales",
    python_callable=load_daily_sales,
    dag=dag,
)

get_last_date_task >> extract_task >> load_task