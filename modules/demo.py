import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
import json
import boto3
from botocore.exceptions import ClientError
from dataclasses import dataclass



""" SETUP FUNCTIONS """

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






"""--- S3 FUNCTIONS ---"""

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


""" ---POSTGRES FUNCTIONS --- """

def test_postgres_connection(postgres_config):
    try:
        print(">>> Attempting to connect to Postgres...")
        postgres_connection = psycopg2.connect(**postgres_config)
        postgres_connection.close()

        print("Connected to Postgres successfully")
    except Exception as e:
        print(f"[ERROR] - Failed to connect to PostgreSQL: {e} ")



""" --- SELECTING DATASET ---"""
@dataclass(frozen=True)  # Make the class immutable 
class PIIDataSet:
    local_path: str
    file_name: str

    @staticmethod
    def select_dataset(use_sample: bool):

        main_dataset = PIIDataSet(local_path="data/raw/pii_dataset.csv", file_name="pii_dataset.csv")
        sample_dataset = PIIDataSet(local_path="data/raw/sample_dataset.csv", file_name="sample_dataset.csv")

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
    silver_df = df[["document", "name", "email", "phone", "len"]]
    silver_df = silver_df.copy()
    
    # Remove whitespace from name column
    silver_df["name"] = silver_df["name"].str.strip()

    # Convert email characters to lowercase 
    silver_df["email"] = silver_df["email"].str.lower()

    print("Transformation in Silver layer completed successfully")

    return silver_df




""" --- DATA VALIDATION CHECKS ---"""

def validate_data(df, contract_path):
    
    if not os.path.exists(contract_path):
        raise FileNotFoundError(f"[ERROR] - Data contract '{contract_path}' not found.")

    with open(contract_path, "r") as file:
        contract = json.load(file)

    contract_name = contract.get("contract_name", "")
    validation_rules = contract.get("validation_rules", {})
    schema = contract.get("schema", {})
    expected_columns = schema.get("columns", [])

    # Bronze-to-Silver: data validation (structural)
    if "BronzeToSilver" in contract_name:
        print("\n>>> Performing bronze-to-silver data validation checks...")

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












""" --- MAIN WORKFLOW ---""" 

def run_data_pipeline(USE_SAMPLE_DATA=False):
    # -- SETUP --
    
    # -- 1. Load configs
    aws_config, bucket_config, postgres_config = load_env_variables()


    # -- 2. Initialize S3 buckets 
    s3_client = initialize_s3_client(aws_config)

    # -- 3. Test connection to Postgres
    test_postgres_connection(postgres_config)

    # --- BRONZE LAYER ---

    # -- 1. Check if bucket exists
    check_if_bucket_exists(s3_client, bucket_config["BRONZE_BUCKET"], aws_config["AWS_REGION"])


    # -- 2. Select + process dataset
    selected_dataset = PIIDataSet.select_dataset(USE_SAMPLE_DATA)

    upload_file_to_s3(s3_client, 
                      selected_dataset.local_path, 
                      bucket_config["BRONZE_BUCKET"], 
                      selected_dataset.file_name)

    # -- 3. Download the file from S3
    bronze_df = download_file_from_s3(
        s3_client,
        bucket_config["BRONZE_BUCKET"],
        selected_dataset.file_name,
        "data/raw/bronze_csv_file.csv",
    )

    # -- 4. Validate the data 
    BronzeToSilver_DataContract     =   "contracts/01_B2S_DataContract.json" 

    validate_data(bronze_df, BronzeToSilver_DataContract)




    # --- SILVER LAYER --- 

    # -- 1. Check if silver bucket exists 
    check_if_bucket_exists(s3_client, bucket_config["SILVER_BUCKET"], aws_config["AWS_REGION"])

    # -- 2. Transform raw data from bronze to silver layer  
    silver_df = transform_data(bronze_df)

    # -- 3. Validate silver data 
    SilverToGold_DataContract       =   "contracts/02_S2G_DataContract.json"
    validate_data(silver_df, SilverToGold_DataContract)

    # -- 4. Save silver data to S3 bucket 
    silver_file_name = "data/transformed/silver_layer.csv"
    silver_df.to_csv(silver_file_name, index=False)
    upload_file_to_s3(s3_client, silver_file_name, bucket_config["SILVER_BUCKET"], silver_file_name)

    print("Transformed data uploaded successfully to silver bucket")
     



if __name__=="__main__":
    run_data_pipeline()