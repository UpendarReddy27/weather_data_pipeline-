import boto3
import pandas as pd
import requests
from pathlib import Path
import os
from datetime import datetime
from dotenv import load_dotenv
import json
# Same folder as this script — avoids Windows backslash + \U escape issues
# # BASE_DIR = Path(__file__).resolve().parent
# file_name = f"extracted_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
# csv_path = f"C:\Users\kvred\OneDrive\Desktop\weather_data_pipeline\{file_name}"
file_name = f"extracted_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
json_path = f"C:/Users/kvred/OneDrive/Desktop/weather_data_pipeline/{file_name}"

load_dotenv()
url = "https://api.open-meteo.com/v1/forecast?latitude=41.8781&longitude=-87.6298&current=temperature_2m,precipitation,wind_speed_10m,weather_code&timezone=America/Chicago&temperature_unit=fahrenheit"

response = requests.get(url)
response.raise_for_status()
data = response.json()

json_string = json.dumps(data)

with open(f"{json_path}", "w") as f:
    f.write(json_string)
s3 = boto3.client("s3",aws_access_key_id=os.environ.get("access_key"),aws_secret_access_key=os.environ.get("secret_key"))
s3.upload_file(
    Filename=str(json_path),  # local file path (required)
    Bucket="weather-data-pipeline-v1",  # bucket name in S3 console (required)
    Key=f"raw_data/{file_name}",  # object path inside the bucket (required)
)

