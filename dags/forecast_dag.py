from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.ski_resort_weather_pipeline import ski_resort_weather_pipeline
from pipelines.aws_s3_pipeline import upload_s3_pipeline

default_args = {
    "owner": "Lucas Derr",
    "start_date": datetime(2023, 12, 20),
}

folder_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='etl_weather_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

extract_transform=PythonOperator(
    task_id="weather_extract_transform",
    python_callable=ski_resort_weather_pipeline,
    op_kwargs = {
        "folder_postfix": folder_postfix
    },
    dag=dag
)
upload_s3 = PythonOperator(
    task_id='s3_upload',
    python_callable=upload_s3_pipeline,
    dag=dag


)


extract_transform >> upload_s3