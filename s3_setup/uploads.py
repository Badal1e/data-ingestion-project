import boto3
from datetime import datetime

S3_BUCKET = "s3-data-python-ingestion-bucket"
AWS_REGION = "ap-south-1"

s3 = boto3.client("s3", region_name=AWS_REGION)

file_path = "../sample-data/spotify.csv"
date_str = datetime.now().strftime("%Y-%m-%d")
s3_key = f"raw/spotify_{date_str}.csv"

s3.upload_file(file_path, S3_BUCKET, s3_key)

print(f"Uploaded file to s3://{S3_BUCKET}/{s3_key}")
