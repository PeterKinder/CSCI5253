from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from etl_scripts.transform import transform_data
from etl_scripts.load import load_data
from datetime import datetime
import os
SOURCE_URL= 'https://data.austintexas.gov/api/views/9t4d-g238/rows.csv?date=20231118&accessType=DOWNLOAD'
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
CSV_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/downloads'
CSV_TARGET_FILE = CSV_TARGET_DIR + '/outcomes_{{ ds }}.csv'
PQ_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/processed'
with DAG(
    dag_id="outcomes_dag",
    start_date = datetime(2023, 11, 20),
    schedule_interval = "@daily"
) as dag:
    extract = BashOperator(
        task_id="extract",
        bash_command=f"curl --create-dirs -o {CSV_TARGET_FILE} {SOURCE_URL}"
    )
    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        op_kwargs={
            'source_csv': CSV_TARGET_FILE,
            'target_dir': PQ_TARGET_DIR
        }
    )
    load_outcome_animal_dim = PythonOperator(
        task_id="load_outcome_animal_dim",
        python_callable=load_data,
        op_kwargs={
            'table_file': PQ_TARGET_DIR + '/outcome_animal_dim.parquet',
            'table_name': 'outcome_animal_dim',
            'key': 'animal_id'
        }
    )
    extract >> transform >> load_outcome_animal_dim