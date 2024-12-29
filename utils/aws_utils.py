import boto3
from boto3.exceptions import ClientError

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


def download_file_from_s3(s3_client, bucket_name, file_name, file_path):
    try:
        
        s3_client.download_file(bucket_name, file_name, file_path)
        print(f"File '{file_name}' downloaded successfully to '{file_path}'.")

        return pd.read_csv(file_path)
    
    except ClientError as e:
        raise RuntimeError(f"[ERROR] - Unable to download file '{file_name}': {e}")
