import os

from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

from ingest_script import ingest_to_postgres


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')


def previous_month(any_day):
    return (any_day.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")

local_workflow = DAG(
    dag_id="data_ingestion_postgres_dag",
    description="Download monthly New York Taxi data and ingest it to a local Postgres instance.",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    tags=['dtc-de'],
    user_defined_filters={
        'previous_month': previous_month
    }
)

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data/' 
FILE_NAME = 'yellow_tripdata_{{ dag_run.logical_date | previous_month }}.csv'
URL_FULL = URL_PREFIX + FILE_NAME
TARGET_FOLDER = AIRFLOW_HOME + '/csv/'
TARGET_FILE = TARGET_FOLDER + FILE_NAME
TABLE_NAME = 'staging.yellow_taxi_{{ dag_run.logical_date | previous_month }}'

with local_workflow:
    download_task = BashOperator(
        task_id='download_dataset',
        bash_command=f'curl -sSLf -C - -o {TARGET_FILE} --create-dirs {URL_FULL}'
        # -C -: don't download again if it already exists (only for DEV, idempotence possibly violated)
        # -sS: silent, bu show errors
        # -L: follow HTTP/S redirects
        # -f: fail when resource is not available (404 error)
        # -o: output path
        # --create-dirs: create missing folders in the output path
    )

    # LocalExecutor doesn't support DockerOperator
    """ ingest_to_postgres = DockerOperator(
        task_id='ingest_to_postgres',
        image='ingest_pg:v001',
        #api_version='1.30',
        api_version='auto',
        auto_remove=True,
        environment={
            'PG_HOST': PG_HOST,
            'PG_USER': PG_USER,
            'PG_PASSWORD': PG_PASSWORD,
            'PG_PORT': PG_PORT,
            'PG_DATABASE': PG_DATABASE
        },
        mounts=[(AIRFLOW_HOME+'/ingest-worker', '/app/code'),
                (TARGET_FOLDER, '/app/csv')],
        #volumes=[AIRFLOW_HOME+'/ingest-worker:/app/code',TARGET_FOLDER+':/app/csv'],
        command='python {AIRFLOW_HOME}/ingest-worker/ingest_taxi_data.py --file={TARGET_FILE} --table={TABLE_NAME}',
        #docker_url='tcp://docker-socket-proxy:2375',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    ) """

    ingest_to_postgres = PythonOperator(
        task_id="ingest_to_postgres",
        python_callable=ingest_to_postgres,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME,
            csv_file=TARGET_FILE
        ),
    )

download_task >> ingest_to_postgres