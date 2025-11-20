from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

PROJECT_ID = "hybrid-creek-475214-a9"      
REGION = "us-central1"
BUCKET = "ritika-leap-bucket"             

BQ_DATASET = "healthcare_analytics"
BQ_RAW_TABLE = "visits_raw"

RAW_FILE = "raw/healthcare_dataset.csv"
ENRICHED_DIR = "processed/healthcare_enriched"  # Dataproc output folder in GCS

DATAPROC_CLUSTER = "ritika-leap-cluster"

default_args = {
    "owner": "ritika",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="healthcare_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="LEAP pipeline: GCS -> BigQuery -> Dataproc",
) as dag:

    start = EmptyOperator(task_id="start")

    load_to_bq = GCSToBigQueryOperator(
        task_id="load_csv_to_bigquery",
        bucket=BUCKET,
        source_objects=[RAW_FILE],
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    pyspark_job = {
        "main_python_file_uri": f"gs://{BUCKET}/scripts/healthcare_etl.py",
        "args": [
            f"--bucket={BUCKET}",
            f"--input_path={RAW_FILE}",
            f"--output_path={ENRICHED_DIR}",
        ],
    }

    run_dataproc = DataprocSubmitJobOperator(
        task_id="run_dataproc_etl",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": DATAPROC_CLUSTER},
            "pyspark_job": pyspark_job,
        },
    )

    end = EmptyOperator(task_id="end")

    start >> load_to_bq >> run_dataproc >> end
