import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.aws_utils import initialize_s3_client, check_if_bucket_exists, upload_file_to_s3, download_file_from_s3
from utils.data_selector import PIIDataSet
from utils.data_validation import validate_data
from utils.env_utils import load_env_variables
from utils.postgres_utils import initialize_postgres, test_postgres_connection, load_data_into_postgres, validate_postgres_load
from utils.transformer import transform_data




TESTING_DATA_CONTRACT_MODE      = "Bronze_To_Silver"


# A. Test the B2S data contract only
if TESTING_DATA_CONTRACT_MODE == "Bronze_To_Silver":
    bronze_use_sample_data = True
    silver_use_sample_data = False

# B. Test the S2G data contract only
elif TESTING_DATA_CONTRACT_MODE == "Silver_To_Gold":
    bronze_use_sample_data = False
    silver_use_sample_data = True


# C. Test both contracts 
elif TESTING_DATA_CONTRACT_MODE == "Both":
    bronze_use_sample_data = False
    silver_use_sample_data = False

else:
    print(f"[ERROR] -- Unable to detect which test this is...: {TESTING_DATA_CONTRACT_MODE}")






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
    "pii_data_pipeline_dag",
    default_args=default_args,
    description="A DAG for validating + processing PII datasets",
    schedule_interval=None,
    start_date=datetime(2024, 12, 29),
    catchup=False,
) as dag:

    # Task 1: Load environment variables
    def load_env_task():
        aws_config, bucket_config, postgres_config = load_env_variables(SHOW_ENV_VARIABLES=False)
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
        dataset     =   PIIDataSet.select_dataset(layer="bronze", use_sample=bronze_use_sample_data)
        file_path   =   f"/opt/airflow/{dataset.file_path}"

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
        
        # Use the main dataset, or sample one for testing 
        silver_dataset                  = PIIDataSet.select_dataset(layer="silver", use_sample=silver_use_sample_data)
        docker_container_silver_path    = f"/opt/airflow/{silver_dataset.file_path}"
        
        # Read dataset from silver zone in Docker container 

        if not os.path.exists(docker_container_silver_path):
            raise FileNotFoundError(f"[ERROR] - Unable to find file in Docker container under this filepath: {docker_container_silver_path}")

        silver_df = pd.read_csv(docker_container_silver_path)

        # Transform the raw data
        print(f"Transforming the raw data from bronze to silver ... ")
        silver_df = transform_data(silver_df) 

        # Validate the downloaded Bronze dataset
        print(f"Validating silver data with S2G data contract...")
        SilverToGoldDataContract = "contracts/02_S2G_DataContract.json"
        validate_data(silver_df, SilverToGoldDataContract)
        print(f"Silver data validation passed successfully ")

        # Convert transformed data to CSV
        docker_container_silver_path = f"/opt/airflow/data/2_silver/transformed_pii_dataset.csv"
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
        docker_container_silver_path   = f"/opt/airflow/data/2_silver/{silver_file}"
        silver_df           = download_file_from_s3(
            s3_client, SILVER_BUCKET, silver_file, docker_container_silver_path
        )

        print("Silver dataset successfully loaded into df.")

        # Copy silver data into gold S3 bucket
        gold_file = "user_access_pii_dataset.csv"
        docker_container_gold_path = f"/opt/airflow/data/3_gold/{gold_file}"
        silver_df.to_csv(docker_container_gold_path, index=False)
        upload_file_to_s3(s3_client, docker_container_gold_path, GOLD_BUCKET, gold_file)

        print(f"Transformed data successfully copied to gold S3 bucket '{GOLD_BUCKET}' as '{gold_file}' ")

        gold_df = download_file_from_s3(s3_client, GOLD_BUCKET, gold_file, docker_container_gold_path)

        # Ensure Postgres target objects exist
        print("Ensuring Postgres objects exist...")
        initialize_postgres(postgres_config)

        # Load data into Postgres
        print("Loading data into Postgres...")
        load_data_into_postgres(postgres_config, gold_df)

        # Validate data in Postgres
        print("Validating data in Postgres...")
        validate_postgres_load(
            postgres_config,
            expected_row_count=len(gold_df),
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