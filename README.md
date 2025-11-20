Healthcare ETL Pipeline on Google Cloud
Objective

Build an end-to-end data pipeline that ingests, processes, transforms, and analyzes healthcare data using Google Cloud Platform (GCP) services:

Cloud Storage (GCS)

BigQuery

Dataproc (Spark)

Cloud Composer (Airflow)

The pipeline automatically loads raw CSV data into BigQuery, runs a PySpark ETL job on Dataproc, and stores enriched data back into GCS.

        +------------------+
        |  Kaggle Dataset  |
        +------------------+
                  |
                  v
+----------------------------------+
|     Google Cloud Storage (GCS)   |
| raw/healthcare_dataset.csv       |
+----------------------------------+
                  |
                  v
+----------------------------------+
|     Cloud Composer (Airflow)     |
|  DAG: healthcare_pipeline        |
+----------------------------------+
        |                  |
        v                  v
+---------------+   +-------------------+
|  BigQuery     |   |   Dataproc Spark  |
| visits_raw    |   | healthcare_etl.py |
+---------------+   +-------------------+
                           |
                           v
              +--------------------------------+
              | GCS – processed/enriched output |
              +--------------------------------+

Components

Bucket: ritika-leap-bucket

raw/healthcare_dataset.csv – original Kaggle dataset

processed/healthcare_enriched/ – enriched output from Dataproc

scripts/healthcare_etl.py – PySpark ETL script

BigQuery

Dataset: healthcare_analytics

Table: visits_raw (loaded by Airflow from GCS)

Dataproc

Cluster: ritika-leap-cluster

Job: healthcare_etl PySpark job reading/writing GCS

Composer / Airflow

Environment: ritika-leap-composer

DAG: healthcare_pipeline

start

load_csv_to_bigquery (GCSToBigQueryOperator)

run_dataproc_etl (DataprocSubmitJobOperator)

end

Data Flow

Ingestion
The raw healthcare CSV is placed in GCS under raw/healthcare_dataset.csv.

Orchestration (Composer)
A Composer DAG healthcare_pipeline is triggered manually.

Load to BigQuery
GCSToBigQueryOperator loads the CSV from GCS into
healthcare_analytics.visits_raw with schema autodetect.

Batch Transformation (Dataproc)
DataprocSubmitJobOperator runs healthcare_etl.py on a Dataproc cluster.
The Spark job:

Reads the CSV from GCS

Converts admission and discharge dates

Calculates length_of_stay_days

Renames columns into a clean snake_case schema

Writes an enriched CSV to processed/healthcare_enriched/ in GCS.

Analysis
BigQuery SQL queries are used to calculate metrics such as:

Average billing and length of stay by medical condition

Revenue by insurance provider

Billing patterns by admission type and hospital.

How to Run

Upload the raw dataset to GCS:
gs://ritika-leap-bucket/raw/healthcare_dataset.csv

Ensure:

BigQuery dataset healthcare_analytics exists.

Dataproc cluster ritika-leap-cluster is running.

healthcare_etl.py is stored at
gs://ritika-leap-bucket/scripts/healthcare_etl.py.

Deploy DAG file healthcare_pipeline_dag.py to the Composer dags/ folder.

In Airflow:

Unpause healthcare_pipeline

Trigger the DAG

After a successful run:

Check visits_raw table in BigQuery

Check processed/healthcare_enriched/ in GCS

Run the sample SQL queries from the SQL section.

Optional Enhancements

Load the enriched CSV back into BigQuery as visits_enriched.

Build a Looker Studio / Tableau dashboard on top of BigQuery.

Add a Vertex AI model to predict high-billing patients as an optional ML step.
