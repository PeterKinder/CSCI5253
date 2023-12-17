from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from etl_scripts.load import load_data
from etl_scripts.load import load_fact_table
from etl_scripts.extract import extract
from datetime import datetime
import os
EXTRACT_TARGET_FILE = 'data/{{ ds }}/downloads/outcomes_{{ ds }}.parquet'
TRANSFORM_TARGET_DIR = 'data/{{ ds }}/processed'
#init_actions_uris sourced from: https://github.com/GoogleCloudDataproc/initialization-actions/blob/master/python/pip-install.sh
CLUSTER_CONFIG = ClusterGenerator(
    project_id=os.environ['GCP_PROJECT_ID'],
    region='us-central1',
    cluster_name=os.environ['GCP_CLUSTER_NAME'],
    master_machine_type='n2-standard-2',
    worker_machine_type='n2-standard-2',
    num_workers=2,
    master_disk_size=32,
    worker_disk_size=32,
    metadata={'PIP_PACKAGES': 'boto3==1.34.1'},
    init_actions_uris= ['gs://csci5253-lab4/pip-install.sh']
).make()
PYSPARK_JOB = {
    "reference": {"project_id": os.environ['GCP_PROJECT_ID']},
    "placement": {"cluster_name": os.environ['GCP_CLUSTER_NAME']},
    "pyspark_job": {"main_python_file_uri": 'gs://csci5253-lab4/transform.py', "args": ['-s', EXTRACT_TARGET_FILE, '-t', TRANSFORM_TARGET_DIR]}
}
with DAG(
    dag_id="outcomes_dag",
    start_date = datetime(2023, 12, 16),
    end_date = datetime(2023, 12, 16),
    schedule_interval = "@daily",
    max_active_runs = 1
) as dag:
    def branch_init(**kwargs):
        # Sourced from: https://stackoverflow.com/questions/58976215/airflow-scheduling-how-to-run-initial-setup-task-only-once
        date = kwargs['data_interval_start']
        if date == dag.start_date:
            return 'create_cluster'
        else:
            return 'skip_task_init'
    def branch_term(**kwargs):
        # Sourced from: https://stackoverflow.com/questions/58976215/airflow-scheduling-how-to-run-initial-setup-task-only-once
        date = kwargs['data_interval_start']
        if date == dag.end_date:
            return 'delete_cluster'
        else:
            return 'skip_task_term'
    branch_task_init = BranchPythonOperator(
        task_id='branch_task_init', 
        python_callable=branch_init,
        provide_context=True
    )
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        project_id=os.environ['GCP_PROJECT_ID'],
        cluster_config=CLUSTER_CONFIG,
        region='us-central1',
        cluster_name=os.environ['GCP_CLUSTER_NAME']
    )
    skip_task_init = DummyOperator(
        task_id='skip_task_init'
    )
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
        trigger_rule='one_success',
        op_kwargs={
            'target_file': EXTRACT_TARGET_FILE,
            'start_date': '2023-11-01',
            'execution_date': '{{ ds }}'
        }
    )
    transform = DataprocSubmitJobOperator(
        task_id="transform", 
        job=PYSPARK_JOB, 
        region='us-central1', 
        project_id=os.environ['GCP_PROJECT_ID']
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
    branch_task_term = BranchPythonOperator(
        task_id='branch_task_term', 
        python_callable=branch_term,
        provide_context=True
    )
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=os.environ['GCP_PROJECT_ID'],
        cluster_name=os.environ['GCP_CLUSTER_NAME'],
        region='us-central1',
    )
    skip_task_term = DummyOperator(
        task_id='skip_task_term'
    )
    branch_task_init >> [create_cluster, skip_task_init] >> extract >> \
        transform >> [load_outcome_animal_dim, load_outcome_date_dim, load_outcome_type_dim] >> \
        load_outcome_fact_table >> branch_task_term >> [delete_cluster, skip_task_term]