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

    # Get S3 event details
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    logger.info(f"Triggered by file: {key}")

    # Read raw file from S3
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")

    rows = None

    # Try parsing JSON first (API ingestion)
    try:
        rows = json.loads(content)

        # ensure list form
        if isinstance(rows, dict):
            rows = [rows]

        logger.info("Detected JSON format")

    except Exception:
        # If JSON fails → treat as CSV
        logger.info("Detected CSV format")
        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)

    valid_records = []
    invalid_records = []

    # Validate each row
    for row in rows:
        if is_valid(row):
            valid_records.append(row)
        else:
            invalid_records.append(row)

    # Processed file naming (prefix preserved)
    filename = key.split("/")[-1].rsplit(".", 1)[0] + ".json"

    if valid_records:
        s3.put_object(
            Bucket=bucket,
            Key=f"processed/{filename}",
            Body=json.dumps(valid_records)
        )

    if invalid_records:
        s3.put_object(
            Bucket=bucket,
            Key=f"error/{filename}",
            Body=json.dumps(invalid_records)
        )

    logger.info(f"Valid records: {len(valid_records)}")
    logger.info(f"Invalid records: {len(invalid_records)}")

    return {"statusCode": 200}
