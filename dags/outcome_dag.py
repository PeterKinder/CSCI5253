from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from etl_scripts.transform import transform_data
from etl_scripts.load import load_data
from etl_scripts.load import load_fact_table
from etl_scripts.extract import extract
from datetime import datetime
import os
SOURCE_URL= 'https://data.austintexas.gov/api/views/9t4d-g238/rows.csv?date=20231118&accessType=DOWNLOAD'
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
CSV_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/downloads'
CSV_TARGET_FILE = CSV_TARGET_DIR + '/outcomes_{{ ds }}.csv'
PQ_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/processed'
with DAG(
    dag_id="outcomes_dag",
    start_date = datetime(2023, 11, 19),
    schedule_interval = "@daily"
) as dag:
    """
    extract = BashOperator(
        task_id="extract",
        bash_command=f"curl --create-dirs -o {CSV_TARGET_FILE} {SOURCE_URL}"
    )
    """
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
        op_kwargs={
            'target_dir': CSV_TARGET_DIR,
            'target_file': CSV_TARGET_FILE,
            'start_date': '2023-11-19',
            'execution_date': '{{ ds }}'
        }
    )
    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        op_kwargs={
            'source_csv': CSV_TARGET_FILE,
            'target_dir': PQ_TARGET_DIR,
            'current_date': '{{ ds }}'
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
    load_outcome_date_dim = PythonOperator(
        task_id="load_outcome_date_dim",
        python_callable=load_data,
        op_kwargs={
            'table_file': PQ_TARGET_DIR + '/outcome_date_dim.parquet',
            'table_name': 'outcome_date_dim',
            'key': 'outcome_date_id'
        }
    )
    load_outcome_type_dim = PythonOperator(
        task_id="load_outcome_type_dim",
        python_callable=load_data,
        op_kwargs={
            'table_file': PQ_TARGET_DIR + '/outcome_type_dim.parquet',
            'table_name': 'outcome_type_dim',
            'key': 'outcome_type_id'
        }
    )
    load_outcome_fact_table = PythonOperator(
        task_id="load_outcome_fact_table",
        python_callable=load_fact_table,
        op_kwargs={
            'table_file': PQ_TARGET_DIR + '/outcome_fact_table.parquet',
            'table_name': 'outcome_fact_table'
        }
    )
    extract >> transform >> [load_outcome_animal_dim, load_outcome_date_dim, load_outcome_type_dim] >> load_outcome_fact_table