import os
import json
import pandas as pd
from dotenv import load_dotenv
from dataclasses import dataclass
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
import boto3
from botocore.exceptions import ClientError



# Constants
DATABASE_NAME   = "prod_db"
SCHEMA_NAME     = "gold_layer"
TABLE_NAME      = "pii_records_tbl"


# SQL queries 
CREATE_TABLE_QUERY = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
            document    TEXT NOT NULL,
            name        TEXT NOT NULL,
            email       TEXT,
            phone       TEXT,
            len         INT
            );
"""

CREATE_SCHEMA_QUERY = f"""
    CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}
    ;
"""


TRUNCATE_TABLE_QUERY = f"""
    TRUNCATE TABLE {SCHEMA_NAME}.{TABLE_NAME}
    ;
"""

INSERT_DATA_QUERY = f"""
    INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (document, name, email, phone, len)
    VALUES  %s
    ;
"""

GOLD_VALIDATION_QUERY = f"""
    SELECT * 
    FROM {SCHEMA_NAME}.{TABLE_NAME}
    ;
"""




""" SETUP FUNCTIONS """

def load_env_variables(SHOW_ENV_VARIABLES=True):
    
    load_dotenv(dotenv_path="/opt/airflow/.env")


    # Load and validate AWS credentials
    aws_config = {
        "AWS_ACCESS_KEY":   os.getenv("AWS_ACCESS_KEY"),
        "AWS_SECRET_KEY":   os.getenv("AWS_SECRET_KEY"),
        "AWS_REGION":       os.getenv("AWS_REGION"),
    }

    for key, value in aws_config.items():
        if not value:
            raise ValueError(f"[ERROR] - Missing required AWS environment variable: {key}")



    # Load S3 bucket names
    bucket_config = {
        "BRONZE_BUCKET":    os.getenv("BRONZE_BUCKET"),
        "SILVER_BUCKET":    os.getenv("SILVER_BUCKET"),
        "GOLD_BUCKET":      os.getenv("GOLD_BUCKET"),
    }

    for key, value in bucket_config.items():
        if not value:
            raise ValueError(f"[ERROR] - Missing required bucket name: {key}")



    # Load Postgres config
    postgres_config = {
        "host":         os.getenv("POSTGRES_HOST"),
        "port":         os.getenv("POSTGRES_PORT"),
        "dbname":       os.getenv("POSTGRES_DB"),
        "user":         os.getenv("POSTGRES_USER"),
        "password":     os.getenv("POSTGRES_PASSWORD"),
    }


    for key, value in postgres_config.items():
        if not value:
            raise ValueError(f"[ERROR] - Missing required Postgres config: {key}")
    
    # Display the environment variables
    if SHOW_ENV_VARIABLES:
        print("Showing env variables:\n")
        print(f"AWS Access key:     {aws_config['AWS_ACCESS_KEY'] }")
        print(f"AWS Secret Key:     {aws_config['AWS_SECRET_KEY'] }")
        print(f"AWS Region:         {aws_config['AWS_REGION'] }")
        print(f"Bronze Bucket:      {bucket_config['BRONZE_BUCKET'] }")
        print(f"Silver bucket:      {bucket_config['SILVER_BUCKET'] }")
        print(f"Gold Bucket:        {bucket_config['GOLD_BUCKET'] }")
        print(f"Postgres Config:    {postgres_config}")
    else:
        print("Setting up environment...")

    return aws_config, bucket_config, postgres_config






"""--- S3 FUNCTIONS ---"""

def initialize_s3_client(aws_config):

    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id       =aws_config["AWS_ACCESS_KEY"],
            aws_secret_access_key   =aws_config["AWS_SECRET_KEY"],
            region_name             =aws_config["AWS_REGION"]
        )
        print("S3 client initialized successfully")
        
        return s3_client

    except ClientError as e:
        print(f"[ERROR] - Failed to initialize S3 client: {e}")
        raise



def check_if_bucket_exists(s3_client, bucket_name, region):
    try:
        
        existing_buckets = [
            bucket["Name"] for bucket in s3_client.list_buckets().get("Buckets", [])
        ]

        if bucket_name not in existing_buckets:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
            print(f"Bucket '{bucket_name}' created successfully.")

        else:
            print(f"Bucket '{bucket_name}' already exists.")
    
    except ClientError as e:
        raise RuntimeError(f"[ERROR] - Unable to create/check bucket '{bucket_name}': {e}")




def upload_file_to_s3(s3_client, file_path, bucket_name, file_name):
    try:
        
        s3_client.upload_file(file_path, bucket_name, file_name)
        print(f" File '{file_name}' uploaded successfully to bucket '{bucket_name}'.")
    
    except ClientError as e:
        raise RuntimeError(f"[ERROR] - Unable to upload file '{file_name}': {e}")


def download_file_from_s3(s3_client, bucket_name, file_name, local_path):
    try:
        
        s3_client.download_file(bucket_name, file_name, local_path)
        print(f"File '{file_name}' downloaded successfully to '{local_path}'.")

        return pd.read_csv(local_path)
    
    except ClientError as e:
        raise RuntimeError(f"[ERROR] - Unable to download file '{file_name}': {e}")


""" ---POSTGRES FUNCTIONS --- """

def test_postgres_connection(postgres_config):
    try:
        print(">>> Attempting to connect to Postgres...")
        postgres_connection = psycopg2.connect(**postgres_config)
        postgres_connection.close()

        print("Connected to Postgres successfully")
    except Exception as e:
        print(f"[ERROR] - Failed to connect to PostgreSQL: {e} ")

def initialize_postgres(postgres_config, truncate_table=True):
    
    
    try:
        
        conn = psycopg2.connect(**postgres_config)
        cursor = conn.cursor()


        # Create schema 
        cursor.execute(CREATE_SCHEMA_QUERY)

        # Create table
        cursor.execute(CREATE_TABLE_QUERY)
        

        # Truncate table if it contains data (i.e. truncate + load pattern)
        if truncate_table:
            cursor.execute(TRUNCATE_TABLE_QUERY)

        conn.commit()
        cursor.close()
        conn.close()

        print(f"Postgres schema '{SCHEMA_NAME}' and '{TABLE_NAME}' verified/created successfully")
    
    except Exception as e:
        raise RuntimeError(f"[ERROR] - Unable to verify or create Postgres objects: {e}")



def load_data_into_postgres(postgres_config, df):

    try:
        conn                    = psycopg2.connect(**postgres_config)
        cursor                  = conn.cursor()
        list_of_pii_records     = df.values.tolist()
        
        # Use `execute_values` for batch inserts
        execute_values(cursor, INSERT_DATA_QUERY, list_of_pii_records)
        conn.commit()
        cursor.close()
        conn.close()
        
        print(">>> Data successfully loaded into Postgres.")

    except Exception as e:
        raise RuntimeError(f"[ERROR] - Failed to load data into Postgres: {e}")


def validate_postgres_load(postgres_config, expected_row_count, expected_columns):
    
    try:
        conn                = psycopg2.connect(**postgres_config)
        cursor              = conn.cursor()
        cursor.execute(GOLD_VALIDATION_QUERY)
        rows                = cursor.fetchall()
        actual_row_count    = len(rows)

        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()

        # Log row and column counts for debugging
        print(f"Postgres row count:     {actual_row_count},    Expected: {expected_row_count}")
        print(f"Postgres columns:       {columns},      Expected: {expected_columns}")

        # Validate row count
        if actual_row_count != expected_row_count:
            raise ValueError(
                f"[ERROR] - Row count mismatch in Postgres: Expected {expected_row_count}, Found {actual_row_count}"
            )

        # Validate column names
        if columns != expected_columns:
            raise ValueError(
                f"[ERROR] - Column mismatch in Postgres: Expected {expected_columns}, Found {columns}"
            )

        print("Postgres validation passed successfully.")

    except Exception as e:
        raise RuntimeError(f"[ERROR] - Postgres validation failed: {e}")



""" --- SELECTING DATASET ---"""
@dataclass(frozen=True)  # Make the class immutable 
class PIIDataSet:
    local_path: str
    file_name: str

    @staticmethod
    def select_dataset(use_sample: bool):

        main_dataset    = PIIDataSet(local_path="data/raw/pii_dataset.csv", file_name="pii_dataset.csv")
        sample_dataset  = PIIDataSet(local_path="data/raw/sample_dataset.csv", file_name="sample_dataset.csv")

        if use_sample:
            print("\nUsing 'sample' dataset for this data workflow...")
            return sample_dataset
        
        else:
            print("\nUsing 'main' dataset for this data workflow...")
            return main_dataset

        


""" --- TRANSFORMATION FUNCTIONS ---""" 

def transform_data(df):
    print("\n>>> Transforming raw data ...")
    
    # Extract + transform relevant columns
    required_columns    = ["document", "name", "email", "phone", "len"]
    df                  = df[required_columns]
    
    # Remove whitespace from name column
    df["name"]          = df["name"].str.strip()

    # Convert email characters to lowercase 
    df["email"]         = df["email"].str.lower()

    print("Transformation in Silver layer completed successfully")

    return df




""" --- DATA VALIDATION CHECKS ---"""

def validate_data(df, contract_path):
    
    if not os.path.exists(contract_path):
        raise FileNotFoundError(f"[ERROR] - Data contract '{contract_path}' not found.")

    with open(contract_path, "r") as file:
        print(f"Contract path: {contract_path}")
        contract = json.load(file)

    contract_name       = contract.get("contract_name", "")
    validation_rules    = contract.get("validation_rules", {})
    schema              = contract.get("schema", {})
    expected_columns    = schema.get("columns", [])

    # Bronze-to-Silver: data validation (structural)
    if "BronzeToSilver" in contract_name:
        print("\n>>> Performing bronze-to-silver data validation checks...")

        # Validate row count
        expected_min_row_count  = validation_rules.get("row_count_min", 0)
        actual_row_count        = len(df)
        
        if actual_row_count < expected_min_row_count:
            raise ValueError(
                f"[ERROR] - Row count validation failed: Expected at least {expected_min_row_count} rows, but found {actual_row_count}."
            )

        # Validate column count
        expected_col_count = validation_rules.get("column_count", len(expected_columns))
        actual_col_count = len(df.columns)
        if actual_col_count != expected_col_count:
            raise ValueError(
                f"[ERROR] - Column count validation failed: Expected {expected_col_count} columns, but found {actual_col_count}."
            )

        print("Validation passed successfully for Bronze-to-Silver contract.")
        return


    # Silver-to-Gold data validation (content)
    if "SilverToGold" in contract_name:
        print("\n>>> Performing Silver-to-Gold data validation checks...")

        # Validate row count
        expected_min_row_count = validation_rules.get("row_count_min", 0)
        actual_row_count = len(df)
        if actual_row_count < expected_min_row_count:
            raise ValueError(
                f"[ERROR] - Row count validation failed: Expected at least {expected_min_row_count} rows, but found {actual_row_count}."
            )

        # Validate column count
        expected_col_count = validation_rules.get("column_count", len(expected_columns))
        actual_col_count = len(df.columns)
        if actual_col_count != expected_col_count:
            raise ValueError(
                f"[ERROR] - Column count validation failed: Expected {expected_col_count} columns, but found {actual_col_count}."
            )

        # Validate column names, types, and constraints
        for col_spec in expected_columns:
            col_name = col_spec["name"]
            col_type = col_spec["type"]
            constraints = col_spec.get("constraints", {})


            # Check for missing columns
            list_of_actual_columns = df.columns
            if col_name not in list_of_actual_columns:
                raise ValueError(f"[ERROR] - Missing required column: '{col_name}'.")

            # Validate data type
            is_column_type_string = pd.api.types.is_string_dtype(df[col_name])

            if col_type == "string" and not is_column_type_string:
                raise TypeError(f"[ERROR] - Column '{col_name}' should be of type 'string'.")
            

            is_column_type_integer = pd.api.types.is_integer_dtype(df[col_name])

            if col_type == "integer" and not is_column_type_integer:
                raise TypeError(f"[ERROR] - Column '{col_name}' should be of type 'integer'.")


            # Validate constraints
            has_null_values = df[col_name].isnull().any()
            if constraints.get("not_null") and has_null_values:
                raise ValueError(f"[ERROR] - Column '{col_name}' contains NULL values.")
            

            has_duplicate_values = df[col_name].duplicated().any()
            if "unique" in constraints and has_duplicate_values:
                raise ValueError(f"[ERROR] - Column '{col_name}' contains duplicate values.")
            


        print("Full validation passed successfully for Silver-to-Gold contract.")
        return

    # Return error if we don't know what contract type it is
    raise ValueError(f"[ERROR] - Unknown contract type: '{contract_name}'.")

# DAG configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=0),
}

with DAG(
    "data_pipeline_dag",
    default_args=default_args,
    description="Data pipeline to validate, transform, and load datasets",
    schedule_interval=None,
    start_date=datetime(2024, 12, 28),
    catchup=False,
) as dag:

    # Task 1: Load environment variables
    def load_env_task():
        aws_config, bucket_config, postgres_config = load_env_variables(SHOW_ENV_VARIABLES=True)
        print(f"Loaded configs: AWS={aws_config}, Buckets={bucket_config}, Postgres={postgres_config}")

    load_env_task = PythonOperator(
        task_id="load_env_variables",
        python_callable=load_env_task,
    )

    # Task 2: Test Postgres connection
    def test_postgres_task():
        _not_needed_1, _not_needed_2, postgres_config = load_env_variables()
        test_postgres_connection(postgres_config)



    test_postgres_task = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_task,
    )

    # Task 3: Upload dataset to Bronze layer
    def run_bronze_layer():
        aws_config, bucket_config, _not_needed = load_env_variables()
        s3_client       =   initialize_s3_client(aws_config)

        BRONZE_BUCKET   =   bucket_config["BRONZE_BUCKET"]
        AWS_REGION      =   aws_config["AWS_REGION"]

        print(f"Checking if bronze bucket '{BRONZE_BUCKET}' exists... ")
        check_if_bucket_exists(s3_client, BRONZE_BUCKET, AWS_REGION)

        # Resolve the full path to the dataset file inside the container
        dataset     =   PIIDataSet.select_dataset(use_sample=False)
        file_path   =   f"/opt/airflow/{dataset.local_path}"

        # Log the resolved file path
        print(f"Resolved dataset file path: {file_path}")

        # Check if the file exists before uploading
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"[ERROR] - Dataset file not found: {file_path}")

        # Read raw data from bronze S3 bucket
        print(f"Reading raw data from '{file_path}'... ")
        bronze_df = pd.read_csv(file_path)

        # Validate against the B2S data contract
        print(f"Validating bronze data with B2S data contract...")
        BronzeToSilverDataContract = "contracts/01_B2S_DataContract.json"
        
        validate_data(bronze_df, BronzeToSilverDataContract)
        print(f"Bronze data validation passed successfully ")


        # Upload the dataset file to the Bronze bucket in S3
        upload_file_to_s3(s3_client, file_path, bucket_config["BRONZE_BUCKET"], dataset.file_name)

    run_bronze_layer_task = PythonOperator(
        task_id="run_bronze_layer",
        python_callable=run_bronze_layer,
    )



    # Task 4: Transform and upload dataset to Silver layer
    def run_silver_layer():
        aws_config, bucket_config, _not_needed = load_env_variables()
        s3_client       = initialize_s3_client(aws_config)
        
        SILVER_BUCKET   = bucket_config["SILVER_BUCKET"]
        AWS_REGION      = aws_config["AWS_REGION"]

        print(f"Checking if bucket '{SILVER_BUCKET}' exists... ")
        check_if_bucket_exists(s3_client, SILVER_BUCKET, AWS_REGION)
        
        # Download the Bronze dataset file from S3 to a local path
        bronze_file                     = "bronze_csv_file.csv"
        docker_container_bronze_path    = f"/opt/airflow/data/raw/{bronze_file}"
        bronze_df                       = download_file_from_s3(
            s3_client, bucket_config["BRONZE_BUCKET"], 
            "pii_dataset.csv", 
            docker_container_bronze_path
        )


        # Transform the raw data
        print(f"Transforming the raw data from bronze to silver ... ")
        silver_df = transform_data(bronze_df) 

        # Validate the downloaded Bronze dataset
        print(f"Validating silver data with S2G data contract...")
        SilverToGoldDataContract = "contracts/02_S2G_DataContract.json"
        validate_data(silver_df, SilverToGoldDataContract)
        print(f"Silver data validation passed successfully ")

        # Convert transformed data to CSV
        docker_container_silver_path = f"/opt/airflow/data/transformed/transformed_pii_dataset.csv"
        silver_df.to_csv(docker_container_silver_path, index=False)

        # Upload the transformed Silver dataset to the Silver bucket in S3
        upload_file_to_s3(s3_client, docker_container_silver_path, SILVER_BUCKET, "transformed_pii_dataset.csv")

    run_silver_layer_task = PythonOperator(
        task_id="run_silver_layer",
        python_callable=run_silver_layer,
    )



    def run_gold_layer():
        aws_config, bucket_config, postgres_config = load_env_variables()
        s3_client       = initialize_s3_client(aws_config)
        
        GOLD_BUCKET     = bucket_config["GOLD_BUCKET"]
        AWS_REGION      = aws_config["AWS_REGION"]

        # Check if Gold S3 bucket exists
        print(f"Checking if bucket '{GOLD_BUCKET}' exists...")
        check_if_bucket_exists(s3_client, GOLD_BUCKET, AWS_REGION)

        # Download Silver dataset from S3 to local path
        SILVER_BUCKET       = bucket_config["SILVER_BUCKET"]
        silver_file         = "transformed_pii_dataset.csv"
        docker_container_silver_path   = f"/opt/airflow/data/transformed/{silver_file}"
        silver_df           = download_file_from_s3(
            s3_client, SILVER_BUCKET, silver_file, docker_container_silver_path
        )

        print("Silver dataset successfully loaded into df.")

        # Ensure Postgres target objects exist
        print("Ensuring Postgres objects exist...")
        initialize_postgres(postgres_config)

        # Load data into Postgres
        print("Loading data into Postgres...")
        load_data_into_postgres(postgres_config, silver_df)

        # Validate data in Postgres
        print("Validating data in Postgres...")
        validate_postgres_load(
            postgres_config,
            expected_row_count=len(silver_df),
            expected_columns=["document", "name", "email", "phone", "len"],
        )

        print("Gold layer process completed successfully.")


    run_gold_layer_task = PythonOperator(
            task_id="run_gold_layer",
            python_callable=run_gold_layer
        )

    # Task dependencies
    # load_env_task >> run_bronze_layer_task >> run_silver_layer_task >> run_gold_layer_task
    load_env_task >> test_postgres_task >> run_bronze_layer_task >> run_silver_layer_task >> run_gold_layer_task