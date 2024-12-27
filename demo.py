import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
import json
import boto3
from botocore.exceptions import ClientError
from dataclasses import dataclass



"""
-- SETUP

1. Load env variables
2. Validate environment variables (AWS + Postgres)
3. Initialize S3 client 

"""



# --- SETUP FUNCTIONS ---
def load_env_variables(SHOW_ENV_VARIABLES=False):
    
    load_dotenv()


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






# --- S3 FUNCTIONS ---

def initialize_s3_client(aws_config):

    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_config["AWS_ACCESS_KEY"],
            aws_secret_access_key=aws_config["AWS_SECRET_KEY"],
            region_name=aws_config["AWS_REGION"]
        )
        print("[INFO] - S3 client initialized successfully")
        
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
            print(f"[INFO] - Bucket '{bucket_name}' created successfully.")

        else:
            print(f"[INFO] - Bucket '{bucket_name}' already exists.")
    
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

# --- SELECTING DATASET ---
@dataclass(frozen=True)  # Make the class immutable 
class PIIDataSet:
    local_path: str
    file_name: str

    @staticmethod
    def select_dataset(use_sample=False):

        main_dataset = PIIDataSet(local_path="pii_dataset.csv", file_name="pii_dataset.csv")
        sample_dataset = PIIDataSet(local_path="sample_dataset.csv", file_name="sample_dataset.csv")

        if use_sample:
            print("\nUsing 'sample' dataset for this data workflow...")
            return sample_dataset
        
        else:
            print("\nUsing 'main' dataset for this data workflow...")
            return main_dataset

        


# --- DATA VALIDATION ---

def validate_data(df, contract_path):
    """Validate data against a JSON data contract."""
    if not os.path.exists(contract_path):
        raise FileNotFoundError(f"[ERROR] - Data contract '{contract_path}' not found.")

    with open(contract_path, "r") as file:
        contract = json.load(file)

    # Validate row count
    row_count_min = contract["validation_rules"].get("row_count_min", 0)
    if len(df) < row_count_min:
        raise ValueError(f"[ERROR] - Row count validation failed: Expected at least {row_count_min} rows.")

    # Validate column count
    expected_columns = contract["schema"]["columns"]
    if len(df.columns) != len(expected_columns):
        raise ValueError(f"[ERROR] - Column count validation failed.")

    # Validate columns
    for col_spec in expected_columns:
        col_name = col_spec["name"]
        col_type = col_spec["type"]
        constraints = col_spec.get("constraints", {})

        if col_name not in df.columns:
            raise ValueError(f"[ERROR] - Missing required column: '{col_name}'.")

        if col_type == "string" and not pd.api.types.is_string_dtype(df[col_name]):
            raise TypeError(f"[ERROR] - Column '{col_name}' should be of type 'string'.")

        if col_type == "integer" and not pd.api.types.is_integer_dtype(df[col_name]):
            raise TypeError(f"[ERROR] - Column '{col_name}' should be of type 'integer'.")

        if constraints.get("not_null") and df[col_name].isnull().any():
            raise ValueError(f"[ERROR] - Column '{col_name}' contains NULL values.")

    print("Data validation passed successfully ")



        



"""

-- Bronze layer 

1. Check if bucket exist. Create it if it doesn't 
2. Check if CSV data is in the bucket. Upload CSV to bucket if it isn't
3. Read data from bronze bucket into pandas df 
4. Validate the data against the bronze-to-silver data contract. Create contract if it doesn't exist.  
5. Proceed to the next stage if validation checks pass

"""




# --- MAIN WORKFLOW --- 


def run_data_pipeline(USE_SAMPLE_DATA=True):
    
    
    # -- 1. Load configs
    aws_config, bucket_config, _ = load_env_variables()


    # -- 2. Initialize S3 buckets 
    s3_client = initialize_s3_client(aws_config)

    # -- 3. Check if bucket exists
    check_if_bucket_exists(s3_client, bucket_config["BRONZE_BUCKET"], aws_config["AWS_REGION"])


    # -- 4. Select + process dataset
    selected_dataset = PIIDataSet.select_dataset(USE_SAMPLE_DATA)

    upload_file_to_s3(s3_client, 
                      selected_dataset.local_path, 
                      bucket_config["BRONZE_BUCKET"], 
                      selected_dataset.file_name)

    # -- 5. Download the file from S3
    df = download_file_from_s3(
        s3_client,
        bucket_config["BRONZE_BUCKET"],
        selected_dataset.file_name,
        "bronze_csv_file.csv",
    )

    # -- 6. Validate the data 
    validate_data(df, "01_B2S_DataContract.json")


if __name__=="__main__":
    run_data_pipeline()