import json
import requests
from datetime import datetime
from config.aws_config import get_s3_client, S3_BUCKET
import yaml

with open("config/app_config.yaml", "r") as f:
    config = yaml.safe_load(f)

RAW_PREFIX = config["s3"]["raw_prefix"]

def upload_from_api():
    url = "https://api.coingecko.com/api/v3/coins/markets"

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 50,
        "page": 1,
        "sparkline": "false"
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise Exception(f"API call failed: {response.status_code}")

    data = response.json()

    date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    s3_key = f"{RAW_PREFIX}crypto_data_{date_str}.json"

    s3 = get_s3_client()

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(data)
    )

    print(f"Uploaded API data to s3://{S3_BUCKET}/{s3_key}")

if __name__ == "__main__":
    upload_from_api()
