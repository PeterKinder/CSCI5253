from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from etl_scripts.transform import transform_data
from etl_scripts.load import load_data
from etl_scripts.load import load_fact_table
from etl_scripts.extract import extract
from datetime import datetime
EXTRACT_TARGET_FILE = 'data/{{ ds }}/downloads/outcomes_{{ ds }}.parquet'
TRANSFORM_TARGET_DIR = 'data/{{ ds }}/processed'
with DAG(
    dag_id="outcomes_dag",
    start_date = datetime(2023, 11, 1),
    schedule_interval = "@daily",
    max_active_runs = 1
) as dag:
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
        op_kwargs={
            'target_file': EXTRACT_TARGET_FILE,
            'start_date': '2023-11-01',
            'execution_date': '{{ ds }}'
        }
    )
    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        op_kwargs={
            'source_csv': EXTRACT_TARGET_FILE,
            'target_dir': TRANSFORM_TARGET_DIR
        }
    )
    load_outcome_animal_dim = PythonOperator(
        task_id="load_outcome_animal_dim",
        python_callable=load_data,
        op_kwargs={
            'table_file': TRANSFORM_TARGET_DIR + '/outcome_animal_dim.parquet',
            'table_name': 'outcome_animal_dim',
            'key': 'animal_id'
        }
    )
    load_outcome_date_dim = PythonOperator(
        task_id="load_outcome_date_dim",
        python_callable=load_data,
        op_kwargs={
            'table_file': TRANSFORM_TARGET_DIR + '/outcome_date_dim.parquet',
            'table_name': 'outcome_date_dim',
            'key': 'outcome_date_id'
        }
    )
    load_outcome_type_dim = PythonOperator(
        task_id="load_outcome_type_dim",
        python_callable=load_data,
        op_kwargs={
            'table_file': TRANSFORM_TARGET_DIR + '/outcome_type_dim.parquet',
            'table_name': 'outcome_type_dim',
            'key': 'outcome_type_id'
        }
    )
    load_outcome_fact_table = PythonOperator(
        task_id="load_outcome_fact_table",
        python_callable=load_fact_table,
        op_kwargs={
            'table_file': TRANSFORM_TARGET_DIR + '/outcome_fact_table.parquet',
            'table_name': 'outcome_fact_table'
        }
    )
    extract >> transform >> [load_outcome_animal_dim, load_outcome_date_dim, load_outcome_type_dim] >> load_outcome_fact_table