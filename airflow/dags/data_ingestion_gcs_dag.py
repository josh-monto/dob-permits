import os
import requests
import pandas as pd
from sodapy import Socrata
from dateutil import parser
from time import sleep
import json

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from google.cloud import bigquery, storage
from airflow.providers.google.cloud.operators.bigquery import (
  BigQueryInsertJobOperator
)
from datetime import datetime, timedelta, timezone

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
EXTERNAL_TABLE = os.environ.get("EXTERNAL_TABLE")
TEMP_TABLE = os.environ.get("TEMP_TABLE")
TABLE_NAME = os.environ.get("TABLE_NAME")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
token_file_path = os.environ.get("API_TOKEN_PATH", "/opt/airflow/nyc_od_app_token.txt")
parquet_file_name = "building_permits"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "building_permits")
API_ENDPOINT = "https://data.cityofnewyork.us/resource/ipu4-2q9a.json"
DATASET_ID = "ipu4-2q9a"#"rbx6-tga4"


def get_last_issued_date():
  # Find latest value of issued date in the BigQuery permits table.
  # If not found, table is empty and current datetime-1825 days (5
  # years) is returned.

  client = bigquery.Client(project=PROJECT_ID)
  dataset_ref = client.dataset(BIGQUERY_DATASET)
  table_ref = dataset_ref.table(TABLE_NAME)

  try:
    client.get_table(table_ref)
  except:
    # there is a lot of data so for initial pull will only pull for last 5 years
    return (datetime.now(timezone.utc) - timedelta(days=1825)).strftime(
      "%Y-%m-%dT00:00:00.000"
    )

  QUERY = f"""
    SELECT MAX(issuance_date) as max_date
    FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{TABLE_NAME}`
  """
  query_job = client.query(QUERY)  # API request
  rows = query_job.result()  # Waits for query to finish
  row = list(rows)[0]

  return row.max_date.strftime("%Y-%m-%dT00:00:00.000")


# pulls all permits from NYC open data's building permits dataset
# after date returned from get_last_issued_date. any task that
# returns a value is stored in xcom, so needed date can be pulled
# from context. NYC open data limits the # of rows per pull, so needs
# to be pulled in batches less than or equal to the limit
# since source data is all string, the method used needs to compare each string
# will take a long time for initial table creation, but daily updates will be quick
def fetch_permits(batch_size, **context):
  with open(token_file_path, "r") as file:
    API_TOKEN = file.read().rstrip()

  ti = context["ti"].xcom_pull(task_ids="get_last_issued_date_task")
  date = datetime.strptime(ti, '%Y-%m-%dT%H:%M:%S.%f') 
  today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
  assert date <= today, "latest issue date later than today"

  client = Socrata("data.cityofnewyork.us", API_TOKEN)
  # headers = {"X-App-Token": API_TOKEN}
  # params={"$limit": batch_size, "$offset": offset, "$where": f"issued_date >= {ti}"}
  df = None
  # pull data for each date in range
  while date <= today:
    offset = 0
    print(f"Fetching records for {date.strftime('%Y-%m-%d')} (offset={offset})")
    # if row number for date is over API limit, use offset to pull data in batches
    try:
      while True:
        results = client.get(
          DATASET_ID, offset=offset, limit=batch_size, where=f"issuance_date = '{date.strftime('%m/%d/%Y')}'"
        )
        if results:
          print("results")
          if offset == 0 and df is None:
            df = pd.DataFrame.from_records(results)
            print(f"df length: {df.shape[0]}")
          else:
            df = pd.concat([df, pd.DataFrame.from_records(results)], ignore_index=True)
            print(f"df length: {df.shape[0]}")
          offset += batch_size
        else:
          print("no results")
          break
    except requests.exceptions.ReadTimeout:
      print("Read Timeout: Wait 5 seconds to continue.")
      sleep(5)
      continue
    date += timedelta(days=1)

  if df is None:
    # create a skip branch to bypass rest of workflow
    return "skip_tasks"
  else:
    print(f"Create file with df of length {df.shape[0]}")
    df.to_parquet(f"{path_to_local_home}/{parquet_file_name}.parquet", index=False)
    return "local_to_gcs_task"

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(**context):
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
  bucket = client.bucket(BUCKET)

  gcs_filename = f"raw/{parquet_file_name}-{datetime.now().strftime('%Y-%m-%d-%H%M%S')}.parquet"
  local_filename = f"{path_to_local_home}/{parquet_file_name}.parquet"
  
  print("uploading to gcs")
  blob = bucket.blob(gcs_filename)
  blob.upload_from_filename(local_filename)

  # need to pull filename for external table creation
  return f"gs://{BUCKET}/{gcs_filename}"

default_args = {
  "owner": "airflow",
  "start_date": days_ago(1),
  "depends_on_past": False,
  "retries": 1,
}

with DAG(
  dag_id="data_ingestion_gcs_dag",
  schedule_interval="@daily",
  default_args=default_args,
  catchup=False,
  max_active_runs=1,
  tags=["dtc-de"],
) as dag:

  get_last_issued_date_task = PythonOperator(
    task_id="get_last_issued_date_task", python_callable=get_last_issued_date
  )

  fetch_permits_task = BranchPythonOperator(
    task_id="fetch_permits_task",
    python_callable=fetch_permits,
    provide_context=True,
    op_kwargs={
      "src": API_ENDPOINT,
      "batch_size": 1000,
    },
  )

  local_to_gcs_task = PythonOperator(
    task_id="local_to_gcs_task",
    python_callable=upload_to_gcs,
  )

  # skip downstream tasks
  skip = EmptyOperator(
    task_id="skip_tasks"
  )

  bq_create_table = BigQueryInsertJobOperator(
    task_id="create_table_task",
    configuration={
      "query": {
        "query": f"""
          CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{BIGQUERY_DATASET}.{TABLE_NAME}`
          (
            borough STRING,
            job_type STRING,
            block STRING,
            lot STRING,
            issuance_date TIMESTAMP,
            permit_si_no INT64,
            location_coordinates STRING,
            gis_nta_name STRING
          )
          PARTITION BY DATE(issuance_date)
          CLUSTER BY gis_nta_name, job_type, borough, block;
        """,
        "useLegacySql": False,
      }
    },
  )

  bq_create_external_table = BigQueryInsertJobOperator(
    task_id="create_external_table_task",
    configuration={
      "query": {
        "query": f"""
          CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.{EXTERNAL_TABLE}`
          OPTIONS (
            format = 'PARQUET',
            uris = ["{{{{ ti.xcom_pull(task_ids="local_to_gcs_task") }}}}"]
          )
        """,
        "useLegacySql": False,
      }
    },
  )

  bq_create_temp_table = BigQueryInsertJobOperator(
    task_id="create_temp_table_task",
    configuration={
      "query": {
        "query": f"""
          CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.{TEMP_TABLE}`
          AS
          SELECT
            borough,
            job_type,
            block,
            lot,
            PARSE_TIMESTAMP('%m/%d/%Y', issuance_date) AS issuance_date,
            CAST(permit_si_no AS INT64) AS permit_si_no,
            CONCAT(gis_latitude,',',gis_longitude) AS location_coordinates,
            gis_nta_name
          FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{EXTERNAL_TABLE}`;
        """,
        "useLegacySql": False,
      }
    },
  )

  bq_merge_tables = BigQueryInsertJobOperator(
    task_id="merge_tables_task",
    configuration={
      "query": {
        "query": f"""
          MERGE INTO `{PROJECT_ID}.{BIGQUERY_DATASET}.{TABLE_NAME}` T
          USING `{PROJECT_ID}.{BIGQUERY_DATASET}.{TEMP_TABLE}` S
          ON T.permit_si_no = S.permit_si_no
          WHEN NOT MATCHED THEN
            INSERT (borough, job_type, block, lot, issuance_date, permit_si_no, location_coordinates, gis_nta_name)
            VALUES (S.borough, S.job_type, S.block, S.lot, S.issuance_date, S.permit_si_no, S.location_coordinates, S.gis_nta_name);
        """,
        "useLegacySql": False,
      }
    },
  )

  join = EmptyOperator(
    task_id="join",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

  get_last_issued_date_task >> fetch_permits_task >> [local_to_gcs_task, skip]
  local_to_gcs_task >> bq_create_table >> bq_create_external_table >> bq_create_temp_table >> bq_merge_tables >> join
  skip >> join