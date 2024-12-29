import os
from dotenv import load_dotenv

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
