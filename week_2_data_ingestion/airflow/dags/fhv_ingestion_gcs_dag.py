import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
URL_PREFIX = 'https://nyc-tlc.s3.amazonaws.com/trip+data/' 
FILE_NAME = 'fhv_tripdata_{{ dag_run.logical_date | iso_month }}.csv'
URL_FULL = URL_PREFIX + FILE_NAME
TARGET_FOLDER = AIRFLOW_HOME + '/csv'
TARGET_FILE = FILE_NAME
TARGET_PARQUET = TARGET_FILE.replace('.csv', '.parquet')
TABLE_NAME = 'fhv_{{ dag_run.logical_date | iso_month }}'

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


def iso_month(any_day):
    return any_day.strftime("%Y-%m")

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "Lorenz",
    "depends_on_past": True,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="fhv_ingestion_gcs_dag",
    description="Download monthly New York For-Hire Vehicle Trip data and ingest it to BigQuery.",
    schedule_interval="0 6 1 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 1, 1),
    max_active_runs=1,
    catchup=True,
    default_args=default_args,
    tags=['dtc-de'],
    user_defined_filters={
        'iso_month': iso_month
    }
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset",
        #bash_command=f"curl -sS {dataset_url} > {path_to_local_home}/{dataset_file}"
        bash_command=f'curl -sSLf -C - -o {TARGET_FOLDER}/{TARGET_FILE} --create-dirs {URL_FULL}'
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f'{TARGET_FOLDER}/{TARGET_FILE}'
        },
    )

    """ local_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="local_to_gcs",
        src=f'{TARGET_FOLDER}/{TARGET_PARQUET}',
        dst=f'raw/{TARGET_PARQUET}',
        bucket=BUCKET,
    ) """

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{TARGET_PARQUET}",
            "local_file": f"{TARGET_FOLDER}/{TARGET_PARQUET}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": TABLE_NAME,
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{TARGET_PARQUET}"],
            },
        },
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task