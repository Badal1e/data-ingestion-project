import json
import boto3
import csv
import io
import logging
import pandas as pd
from validation import clean_dataframe

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

    try:
        data = json.loads(content)
        if isinstance(data, dict):
            data = [data]
        df = pd.DataFrame(data)
        logger.info("Detected JSON format")
    except Exception:
        reader = csv.DictReader(io.StringIO(content))
        df = pd.DataFrame(list(reader))
        logger.info("Detected CSV format")

    clean_df, error_df = clean_dataframe(df)

    filename = key.split("/")[-1].rsplit(".", 1)[0] + ".json"

    if not clean_df.empty:
        s3.put_object(
            Bucket=bucket,
            Key=f"processed/{filename}",
            Body=json.dumps(clean_df.to_dict(orient="records"))
        )

    if not error_df.empty:
        s3.put_object(
            Bucket=bucket,
            Key=f"error/{filename}",
            Body=json.dumps(error_df.to_dict(orient="records"))
        )

    logger.info(f"Valid records: {len(clean_df)}")
    logger.info(f"Invalid records: {len(error_df)}")

    return {"statusCode": 200}
