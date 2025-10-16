from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from airflow.operators.bash import BashOperator
import os

# Path inside the container where we mounted CSVs
RESOURCES_PATH = "/opt/airflow/Resources"

def load_csvs():
    files = [
        "olist_orders_dataset.csv",
        "olist_customers_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
        "olist_geolocation_dataset.csv",
        "product_category_name_translation.csv"
    ]

    for f in files:
        file_path = os.path.join(RESOURCES_PATH, f)
        print(f"Reading {file_path}")
        df = pd.read_csv(file_path)
        print(f"{f} → {len(df)} rows, {len(df.columns)} columns")

with DAG(
    dag_id="AutomatePipeline",
    start_date=datetime(2025, 10, 8),
    schedule_interval="*/2 * * * *",
    catchup=False,
    tags=["olist"]
) as dag1:

    read_task = PythonOperator(
        task_id="read_all_csvs",
        python_callable=load_csvs
    )

with DAG(
    dag_id="olist_etl_bash_dag",
    start_date=datetime(2025, 10, 8),
    schedule_interval=" 0 */1  * * *",
    catchup=False,
    tags=["Extract","load","transform","ETL"]
)as dag2:
    run_etl_script=BashOperator(
        task_id="run_etl_script",
        bash_command="python /opt/airflow/dags/ETL_Pipline.py"

    )

    load_to_db=BashOperator(
        task_id="load_to_db",
        bash_command="python /opt/airflow/dags/LoadFilesIntoDatabase.py"
    )

    run_etl_script >> load_to_db