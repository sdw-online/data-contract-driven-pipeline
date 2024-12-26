import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
import json
import boto3
from boto3.exceptions import Boto3Error



# Load environment variables
load_dotenv()

AWS_ACCESS_TOKEN = os.getenv("AWS_ACCESS_TOKEN")
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



CHECK_ENV_VARIABLES = True

if CHECK_ENV_VARIABLES:
    print("Showing env variables:\n")
    print(f"AWS Access Token:   {AWS_ACCESS_TOKEN}")
    print(f"AWS Secret Key:     {AWS_SECRET_KEY}")
    print(f"AWS Region:         {AWS_REGION}")
    print(f"Bronze Bucket:      {BRONZE_BUCKET}")
    print(f"Silver bucket:      {SILVER_BUCKET}")
    print(f"Gold Bucket:        {GOLD_BUCKET}")
    print(f"Postgres Config:    {POSTGRES_CONFIG}")
else:
    print("Setting up environment...")