import boto3
import yaml

with open("config/app_config.yaml", "r") as f:
    config = yaml.safe_load(f)

AWS_REGION = config["aws"]["region"]
S3_BUCKET = config["s3"]["bucket"]

def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)
