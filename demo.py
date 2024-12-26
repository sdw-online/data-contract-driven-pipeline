import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
import json
import boto3
from boto3.exceptions import Boto3Error



# Load environment variables

AWS_ACCESS_TOKEN = os.getenv("AWS_ACCESS_TOKEN")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("SILVER_BUCKET")
GOLD_BUCKET = os.getenv("GOLD_BUCKET")
BUCKET_NAME = os.getenv("BUCKET_NAME")

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}