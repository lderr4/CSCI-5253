
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from etl_scripts.transform import load_transform
from etl_scripts.load import load_data, load_fact_data


SOURCE_URL = 'https://data.austintexas.gov/api/views/9t4d-g238/rows.csv?accessType=DOWNLOAD'
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
CSV_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/downloads'

CSV_TARGET_FILE = CSV_TARGET_DIR + '/outcomes_{{ ds }}.csv'
PQ_TARGET_FILE = CSV_TARGET_DIR + '/outcomes_{{ ds }}_processed'

with DAG(
    dag_id="outcomes_dag",
    start_date=datetime(2023,11,20),
    schedule_interval="@daily"

) as dag:
    extract = BashOperator(
        task_id="extract",
        bash_command=f"curl --create-dirs -o {CSV_TARGET_FILE} {SOURCE_URL}"
    )
    transform = PythonOperator(
        task_id="transform",
        python_callable=load_transform,
        op_kwargs={
            "source_csv": SOURCE_URL,
            "target_dir": PQ_TARGET_FILE
        }
    )
    load_animal_dim = PythonOperator(
        task_id="animal",
        python_callable=load_data,
        op_kwargs={
            'table_file': f"{PQ_TARGET_FILE}_animal_dim.parquet",
            'table_name': 'animal_dim',
            'key': 'Animal_ID'
        }

    )
    load_outcome_dim = PythonOperator(
        task_id="outcome",
        python_callable=load_data,
        op_kwargs={
            'table_file': f"{PQ_TARGET_FILE}_outcome_dim.parquet",
            'table_name': 'outcome_dim',
            'key': 'Outcome_ID'
        }
    )
    load_time_dim = PythonOperator(
        task_id="time",
        python_callable=load_data,
         op_kwargs={
            'table_file': f"{PQ_TARGET_FILE}_time_dim.parquet",
            'table_name': 'time_dim',
            'key': 'Outcome_ID' 
        }
    )
    load_fact_data = PythonOperator(
        task_id="load_fact_data",
        python_callable=load_fact_data,
        op_kwargs={
            "table_file": f"{PQ_TARGET_FILE}",
            "table_name": "dim_outcomes_fact_table"
        }
    )
    extract >> transform >> [load_animal_dim, load_outcome_dim, load_time_dim] >> load_fact_data
