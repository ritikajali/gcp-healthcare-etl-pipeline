# Healthcare ETL Pipeline on Google Cloud

## Objective

Build an end-to-end data pipeline that ingests, processes, and analyzes a healthcare dataset using Google Cloud services:

- Cloud Storage  
- BigQuery  
- Dataproc  
- Cloud Composer  

## High-Level Architecture

- **Cloud Storage (GCS)** – stores raw and processed CSV files  
- **Cloud Composer (Airflow)** – orchestrates the end-to-end workflow using a DAG  
- **BigQuery** – stores raw healthcare data and supports SQL analytics  
- **Dataproc (Spark)** – runs batch ETL jobs to clean and enrich the dataset and write processed output back to GCS  

## Components

### Google Cloud Storage (GCS)
**Bucket:** `ritika-leap-bucket`
- `raw/healthcare_dataset.csv` – original Kaggle dataset  
- `processed/healthcare_enriched/` – enriched output from Dataproc  
- `scripts/healthcare_etl.py` – PySpark ETL job  

### BigQuery
- **Dataset:** `healthcare_analytics`  
- **Table:** `visits_raw` (loaded by Airflow from GCS)  

### Dataproc
- **Cluster:** `ritika-leap-cluster`  
- **Job:** `healthcare_etl` (PySpark job reading/writing GCS)  

### Cloud Composer / Airflow
- **Environment:** `ritika-leap-composer`  
- **DAG:** `healthcare_pipeline`
  - `start`
  - `load_csv_to_bigquery` (GCSToBigQueryOperator)
  - `run_dataproc_etl` (DataprocSubmitJobOperator)
  - `end`  

## Data Flow

### 1. Ingestion
- Place the raw healthcare CSV in GCS:  
  `gs://ritika-leap-bucket/raw/healthcare_dataset.csv`

### 2. Orchestration (Composer)
- The Airflow DAG **`healthcare_pipeline`** is triggered manually.

### 3. Load to BigQuery
- `GCSToBigQueryOperator` loads CSV into:  
  `healthcare_analytics.visits_raw`  
- Schema uses **autodetect**.

### 4. Batch Transformation (Dataproc)
The Spark job (`healthcare_etl.py`) performs:
- Reads raw CSV from GCS  
- Converts admission/discharge dates  
- Calculates `length_of_stay_days`  
- Renames columns to **snake_case**  
- Writes enriched CSV to:  
  `processed/healthcare_enriched/` in GCS  

### 5. Analysis (BigQuery SQL)
Sample insights include:
- Average billing and length of stay by medical condition  
- Revenue by insurance provider  
- Billing patterns by admission type or hospital  

## How to Run

### Step 1 — Upload Raw Dataset
```
gs://ritika-leap-bucket/raw/healthcare_dataset.csv
```

### Step 2 — Ensure Required Resources Exist
- BigQuery dataset **`healthcare_analytics`**  
- Dataproc cluster **`ritika-leap-cluster`**  
- ETL script uploaded to:
```
gs://ritika-leap-bucket/scripts/healthcare_etl.py
```

### Step 3 — Deploy Composer DAG
Upload `healthcare_pipeline_dag.py` into the Composer **dags/** folder.

### Step 4 — Run from Airflow UI
- Unpause **`healthcare_pipeline`**  
- Trigger the DAG  

### Step 5 — Verify Outputs
- Check **`visits_raw`** table in BigQuery  
- Check processed files in:
```
gs://ritika-leap-bucket/processed/healthcare_enriched/
```
- Run SQL queries for analysis  
