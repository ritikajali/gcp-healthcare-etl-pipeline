import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, datediff

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--input_path", required=True)   
    parser.add_argument("--output_path", required=True)  
    return parser.parse_args()

def main():
    args = parse_args()

    spark = (
        SparkSession
        .builder
        .appName("healthcare_etl")
        .getOrCreate()
    )

    input_uri = f"gs://{args.bucket}/{args.input_path}"
    output_uri = f"gs://{args.bucket}/{args.output_path}"

    # Read original CSV from GCS
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_uri)
    )

    # Convert dates and compute length_of_stay_days
    df_clean = (
        df
        .withColumn("Date of Admission", to_date(col("Date of Admission")))
        .withColumn("Discharge Date", to_date(col("Discharge Date")))
        .withColumn(
            "length_of_stay_days",
            datediff(col("Discharge Date"), col("Date of Admission"))
        )
    )

    # Rename columns to snake_case for analytics
    df_final = (
        df_clean
        .withColumnRenamed("Name", "patient_name")
        .withColumnRenamed("Age", "age")
        .withColumnRenamed("Gender", "gender")
        .withColumnRenamed("Blood Type", "blood_type")
        .withColumnRenamed("Medical Condition", "medical_condition")
        .withColumnRenamed("Date of Admission", "admission_date")
        .withColumnRenamed("Discharge Date", "discharge_date")
        .withColumnRenamed("Doctor", "doctor")
        .withColumnRenamed("Hospital", "hospital")
        .withColumnRenamed("Insurance Provider", "insurance_provider")
        .withColumnRenamed("Billing Amount", "billing_amount")
        .withColumnRenamed("Room Number", "room_number")
        .withColumnRenamed("Admission Type", "admission_type")
        .withColumnRenamed("Medication", "medication")
        .withColumnRenamed("Test Results", "test_results")
    )

    # Write enriched data back to GCS as CSV (single file for convenience)
    (
        df_final
        .coalesce(1)                      # 1 output file
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_uri)
    )

    spark.stop()

if __name__ == "__main__":
    main()
