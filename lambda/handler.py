import json
import boto3
import csv
import io
import logging
from validation import is_valid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

def handler(event, context):
    logger.info("Lambda handler started")

    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    logger.info(f"Triggered by file: {key}")

    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(content))

    valid_records = []
    invalid_records = []

    for row in reader:
        if is_valid(row):
            valid_records.append(row)
        else:
            invalid_records.append(row)

    json_file = key.split("/")[-1].replace(".csv", ".json")

    if valid_records:
        s3.put_object(
            Bucket=bucket,
            Key=f"processed/{json_file}",
            Body=json.dumps(valid_records)
        )

    if invalid_records:
        s3.put_object(
            Bucket=bucket,
            Key=f"error/{json_file}",
            Body=json.dumps(invalid_records)
        )

    logger.info(f"Valid records: {len(valid_records)}")
    logger.info(f"Invalid records: {len(invalid_records)}")

    return {"statusCode": 200}
