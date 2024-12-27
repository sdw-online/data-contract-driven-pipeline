from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
import json
import boto3
from botocore.exceptions import ClientError


# Load environment variables
def load_env_variables(SHOW_ENV_VARIABLES=False):
    load_dotenv()
    aws_config = {
        "AWS_ACCESS_KEY": os.getenv("AWS_ACCESS_KEY"),
        "AWS_SECRET_KEY": os.getenv("AWS_SECRET_KEY"),
        "AWS_REGION": os.getenv("AWS_REGION"),
    }
    bucket_config = {
        "BRONZE_BUCKET": os.getenv("BRONZE_BUCKET"),
        "SILVER_BUCKET": os.getenv("SILVER_BUCKET"),
        "GOLD_BUCKET": os.getenv("GOLD_BUCKET"),
    }
    postgres_config = {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }
    return aws_config, bucket_config, postgres_config


# Initialize S3 client
def initialize_s3_client(aws_config):
    return boto3.client(
        "s3",
        aws_access_key_id=aws_config["AWS_ACCESS_KEY"],
        aws_secret_access_key=aws_config["AWS_SECRET_KEY"],
        region_name=aws_config["AWS_REGION"],
    )


# Test connection to Postgres
def test_postgres_connection(**kwargs):
    _, _, postgres_config = load_env_variables()
    try:
        conn = psycopg2.connect(**postgres_config)
        conn.close()
        print("Postgres connection successful")
    except Exception as e:
        raise RuntimeError(f"Postgres connection failed: {e}")


# Check and create S3 bucket
def check_s3_bucket(bucket_name, aws_config, **kwargs):
    s3_client = initialize_s3_client(aws_config)
    try:
        buckets = [bucket["Name"] for bucket in s3_client.list_buckets()["Buckets"]]
        if bucket_name not in buckets:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": aws_config["AWS_REGION"]},
            )
            print(f"Bucket {bucket_name} created.")
        else:
            print(f"Bucket {bucket_name} already exists.")
    except ClientError as e:
        raise RuntimeError(f"Error with bucket {bucket_name}: {e}")


# Upload file to S3
def upload_to_s3(bucket_name, local_path, s3_key, **kwargs):
    aws_config, _, _ = load_env_variables()
    s3_client = initialize_s3_client(aws_config)
    try:
        s3_client.upload_file(local_path, bucket_name, s3_key)
        print(f"Uploaded {s3_key} to {bucket_name}.")
    except ClientError as e:
        raise RuntimeError(f"Error uploading file: {e}")


# Transform raw data to silver
def transform_data_to_silver(**kwargs):
    df = pd.read_csv("data/raw/bronze_csv_file.csv")
    df["name"] = df["name"].str.strip()
    df["email"] = df["email"].str.lower()
    df.to_csv("data/transformed/silver_layer.csv", index=False)
    print("Transformation to silver completed.")


# Validate data
def validate_data(contract_path, data_path, **kwargs):
    with open(contract_path, "r") as file:
        contract = json.load(file)
    df = pd.read_csv(data_path)
    expected_columns = [col["name"] for col in contract["schema"]["columns"]]
    if not all(col in df.columns for col in expected_columns):
        raise ValueError("Validation failed: Missing columns.")
    print("Validation passed.")


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "data_pipeline_dag",
    default_args=default_args,
    description="A simple data pipeline DAG",
    schedule_interval=None,  # Set to your schedule, e.g., "@daily"
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Load environment variables
    load_env_task = PythonOperator(
        task_id="load_env_variables",
        python_callable=load_env_variables,
    )

    # Test Postgres connection
    test_postgres_task = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_connection,
    )

    # Check Bronze bucket
    check_bronze_bucket_task = PythonOperator(
        task_id="check_bronze_bucket",
        python_callable=check_s3_bucket,
        op_kwargs={"bucket_name": "bronze_bucket_name"},
    )

    # Upload to Bronze bucket
    upload_bronze_task = PythonOperator(
        task_id="upload_to_bronze",
        python_callable=upload_to_s3,
        op_kwargs={"bucket_name": "bronze_bucket_name", "local_path": "data/raw/pii_dataset.csv", "s3_key": "pii_dataset.csv"},
    )

    # Transform to Silver layer
    transform_to_silver_task = PythonOperator(
        task_id="transform_to_silver",
        python_callable=transform_data_to_silver,
    )

    # Validate Bronze to Silver
    validate_b2s_task = PythonOperator(
        task_id="validate_bronze_to_silver",
        python_callable=validate_data,
        op_kwargs={
            "contract_path": "contracts/01_B2S_DataContract.json",
            "data_path": "data/raw/bronze_csv_file.csv",
        },
    )

    # Validate Silver to Gold
    validate_s2g_task = PythonOperator(
        task_id="validate_silver_to_gold",
        python_callable=validate_data,
        op_kwargs={
            "contract_path": "contracts/02_S2G_DataContract.json",
            "data_path": "data/transformed/silver_layer.csv",
        },
    )

    # Define task dependencies
    load_env_task >> test_postgres_task >> check_bronze_bucket_task >> upload_bronze_task
    upload_bronze_task >> validate_b2s_task >> transform_to_silver_task >> validate_s2g_task
