import os
from datetime import datetime
from config.aws_config import get_s3_client, S3_BUCKET
import yaml

with open("config/app_config.yaml", "r") as f:
    config = yaml.safe_load(f)

RAW_PREFIX = config["s3"]["raw_prefix"]

def upload_raw_file(file_path):
    s3 = get_s3_client()

    file_name = os.path.basename(file_path)
    date_str = datetime.now().strftime("%Y-%m-%d")
    s3_key = f"{RAW_PREFIX}{file_name.split('.')[0]}_{date_str}.csv"

    s3.upload_file(file_path, S3_BUCKET, s3_key)
    print(f"Uploaded raw file to s3://{S3_BUCKET}/{s3_key}")

if __name__ == "__main__":
    upload_raw_file("sample-data/spotify.csv")
