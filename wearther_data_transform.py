

import boto3
import pandas as pd
from pathlib import Path
import os
from dotenv import load_dotenv
import json

load_dotenv(
    dotenv_path=Path(__file__).resolve().parent / ".env",  # load .env next to this script
)
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.environ.get("access_key"),  # must match .env names (required)
    aws_secret_access_key=os.environ.get("secret_key"),  # must match .env (required)
)

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"  # CSVs written here; created if missing
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # avoid FileNotFoundError on first run

response = s3.list_objects_v2(
    Bucket="weather-data-pipeline-v1",  # bucket name (required)
    Prefix="raw_data/",  # only objects under this prefix (required)
)

for obj in response.get("Contents", []):
    key = obj["Key"]
    # S3 "folders" are often empty 0-byte keys ending in / — not valid JSON
    if key.endswith("/"):
        continue
    if not key.lower().endswith(".json"):
        continue

    print(key)
    get_object = s3.get_object(Bucket="weather-data-pipeline-v1", Key=key)
    raw = get_object["Body"].read().decode("utf-8")
    if not raw.strip():
        continue
    data = json.loads(raw)

    stem = Path(key).stem  # filename without .json
    csv_name = f"extracted_{stem}.csv"
    csv_path = OUTPUT_DIR / csv_name

    extracted = {
        "time": data["current"]["time"],
        "temperature_2m": data["current"]["temperature_2m"],
        "precipitation": data["current"]["precipitation"],
        "wind_speed_10m": data["current"]["wind_speed_10m"],
        "weather_code": data["current"]["weather_code"],
        "latitude": data["latitude"],
        "longitude": data["longitude"],
        "timezone": data["timezone"],
        "elevation": data["elevation"],
    }

    print(extracted)
    df = pd.DataFrame([extracted])
    df.to_csv(csv_path)
for file in OUTPUT_DIR.glob("*.csv"):
    s3.upload_file(
        Filename=str(file),  # local file path (required)
        Bucket="weather-data-pipeline-v1",  # bucket name in S3 console (required)
        Key=f"transformed_data/{file.name}",  # object path inside the bucket (required)
    )

# # Do not put keys in this file — they were exposed in your terminal log; rotate them in IAM.
# # Set AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY, or run: aws configure
# s3 = boto3.client("s3",aws_access_key_id=os.environ.get("access_key"),aws_secret_access_key=os.environ.get("secret_key"))
# s3.upload_file(
#     Filename=str(csv_path),  # local file path (required)
#     Bucket="weather-data-pipeline-v1",  # bucket name in S3 console (required)
#     Key=f"raw_data/{file_name}",  # object path inside the bucket (required)
# )

# print("File uploaded successfully")
