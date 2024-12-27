import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
import json
import boto3
from botocore.exceptions import ClientError




"""
-- SETUP

1. Load env variables
2. Validate environment variables (AWS + Postgres)
3. Initialize S3 client 

"""


# 1.1. Load environment variables
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("SILVER_BUCKET")
GOLD_BUCKET = os.getenv("GOLD_BUCKET")

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}


# -- 1.2. Validate environment variables


# Display the environment variables
SHOW_ENV_VARIABLES = False

if SHOW_ENV_VARIABLES:
    print("Showing env variables:\n")
    print(f"AWS Access key:     {AWS_ACCESS_KEY}")
    print(f"AWS Secret Key:     {AWS_SECRET_KEY}")
    print(f"AWS Region:         {AWS_REGION}")
    print(f"Bronze Bucket:      {BRONZE_BUCKET}")
    print(f"Silver bucket:      {SILVER_BUCKET}")
    print(f"Gold Bucket:        {GOLD_BUCKET}")
    print(f"Postgres Config:    {POSTGRES_CONFIG}")
else:
    print("Setting up environment...")



# Validate AWS credentials for S3 setup
if not AWS_ACCESS_KEY or not AWS_SECRET_KEY or not AWS_REGION:
    raise ValueError(f"[ERROR] - Missing at least one AWS credential in .env file...")



# Check if all variables for Postgres connection are present
for key, value in POSTGRES_CONFIG.items():
    if not value:
        raise ValueError(f"[ERROR] - Missing at least one Postgres credential for key: {key}")




# 1.3. Initialize S3 client 

try:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    print("[INFO] - S3 client initialized successfully")
except ClientError as e:
    print(f"[ERROR] - Failed to initialize S3 client: {e}")
    raise





"""

-- Bronze layer 

1. Check if bucket exist. Create it if it doesn't 
2. Check if CSV data is in the bucket. Upload CSV to bucket if it isn't
3. Read data from bronze bucket into pandas df 
4. Validate the data against the bronze-to-silver data contract. Create contract if it doesn't exist.  
5. Proceed to the next stage if validation checks pass

"""




# 2.1. Create BRONZE S3 bucket
 
try:
    response = s3_client.list_buckets()
    existing_buckets = [bucket['Name'] for bucket in response.get("Buckets", [])]
    if BRONZE_BUCKET not in existing_buckets:
        print(f"[INFO] - Bucket '{BRONZE_BUCKET}' does not exist. Now creating it... ")
        s3_client.create_bucket(
            Bucket=BRONZE_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
        )
        print(f"[INFO] - Bucket '{BRONZE_BUCKET}' created successfully. ")
    else:
        print(f"Bucket '{BRONZE_BUCKET}' already exists")
except ClientError as e:
    print(f"[ERROR] - Failed to check/create bucket '{BRONZE_BUCKET}': {e} ")
    raise


# 2.2. Upload source CSV file

local_source_file_path = "pii_dataset.csv"
file_name = "pii_dataset.csv"

try:
    response = s3_client.list_objects_v2(Bucket=BRONZE_BUCKET)
    files_in_bucket = [obj["Key"] for obj in response.get("Contents", [])]

    if file_name not in files_in_bucket:
        print(f"File {file_name} does not exist in '{BRONZE_BUCKET}'. Now uploading file to bucket...")
        s3_client.upload_file(local_source_file_path, BRONZE_BUCKET, file_name)
        print(f"File '{file_name}' uploaded successfully")
    else:
        print(f"File already exists in bucket '{BRONZE_BUCKET}' ")
except ClientError as e:
    print(f"Failed to check/upload file in bucket '{BRONZE_BUCKET}': {e}")



# 2.3. Read CSV file into Pandas df

try:
    local_bronze_file_path = "bronze_csv_file.csv"
    print(f"Downloading file '{file_name}' from bucket '{BRONZE_BUCKET}'... ")
    s3_client.download_file(BRONZE_BUCKET, file_name, local_bronze_file_path)
    df = pd.read_csv(local_bronze_file_path) 
    print(f"Loaded bronze data into pandas dataframe successfully.")
    print(df.head())
except ClientError as e:
    print(f"[ERROR] - Unable to read CSV data from '{BRONZE_BUCKET}' bucket into pandas df: {e}")



# 2.4. Validate data against the contract expectations 

b2s_contract_path = "01_B2S_DataContract.json"

try:
    # Load data contract 
    if not os.path.exists(b2s_contract_path):
        raise FileNotFoundError(f"[ERROR] - Data contract '{b2s_contract_path}' not found...")
    
    with open(b2s_contract_path, "r") as contract_file:
        b2s_contract = json.load(contract_file)

    # Display the contract expectations in the console 
    print("\n --- Loading the BRONZE-TO-SILVER data contract...:")
    print(json.dumps(b2s_contract, indent=4))
    print("\nValidating the contract against the following expectations:")

    schema = b2s_contract["schema"]["columns"]
    for col in schema:
        print(f" - Column '{col['name']}': Type = {col['type']}, Constraints: {col.get('constraints', 'None')}")

    validation_rules = b2s_contract["validation_rules"]
    print(f"\n - Min row count: {validation_rules.get('row_count_min', 'Not specified')}")

    print("\n - Starting validation...")


    # -- Check total row count
    expected_min_row_count = validation_rules.get('row_count_min', 0)
    actual_row_count = len(df)
    if actual_row_count < expected_min_row_count:
        raise ValueError(f"Row count validation failed. Expected at least {expected_min_row_count} records in the data...")
    
    # --- Check total column count 
    actual_col_count = len(df.columns)
    expected_col_count = validation_rules.get('column_count', actual_col_count)
    if actual_col_count != expected_col_count:
        raise ValueError(f"Column count validation error. Expected '{expected_col_count}' columns but found '{actual_col_count}' instead... ")

     


except FileNotFoundError as e:
    print(e)
    raise

except ValueError as e:
    print(f"[ERROR] - Data validation failed: {e} ")
    raise

except Exception as e:
    print(f"[ERROR] - Unexpected error during validation: {e}") 