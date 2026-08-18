"""
Description:
Script requests satellite active fire detections (VIIRS_SNPP_SP) from NASA FIRMS in 5-day intervals which is the max, for California, Mediterranean Europe and SE Australia.
Uploaded CSV payloads are stored in MinIO Object Storage.
"""

import requests
import boto3
from datetime import date, timedelta

#API and region config
MAP_KEY = "69a3a371fddfbfd1cda4f9c9f7da00fd"    # NASA FIRMS API Key
SOURCE = "VIIRS_SNPP_SP"    # S-NPP VIIRS 375m active fire instrument
DAY_RANGE = 5   # Query interval range

# Geographic bounding: [min_lon, min_lat, max_lon, max_lat]
REGIONS = {
    "california": "-124.5,32.5,-114,42",
    "mediterranean": "19.0,35.0,29.0,42.0",
    "se_australia": "140.0,-39.0,153.5,-28.0",
}

# observation window
START_DATE = date(2013, 1, 1)
END_DATE = date(2025, 3, 31)

#Initializing S3 client for MinIO object storage
client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="cariappa",
    aws_secret_access_key="D3chamma",
)

#data ingestion loop
for region_name, bbox in REGIONS.items():
    current_date = START_DATE
    while current_date <= END_DATE:
        date_str = current_date.isoformat()
        #construct NASA FIRMS API REST endpoint URL
        url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/{SOURCE}/{bbox}/{DAY_RANGE}/{date_str}"
        #S3 object key prefix pattern
        key = f"firms/{region_name}/{date_str}.csv"
        #check if file already exists in MinIO storage to avoid redundant downloads
        try:
            client.head_object(Bucket="fire-weather-ingestion", Key=key)
            print(f"{region_name} {date_str} already exists, skipping")
            current_date += timedelta(days=DAY_RANGE)
            continue
        except client.exceptions.ClientError:
            pass #File does not exist yet so proceed with API request

        #fetch CSV payload from NASA API
        response = requests.get(url)
        print(region_name, date_str, response.status_code)

        try:
            client.put_object(
                Bucket="fire-weather-ingestion",
                Key=key,
                Body=response.text,
            )
        except Exception as e:
            print(f"  upload FAILED: {e}")
        #Increment date pointer by defined range
        current_date += timedelta(days=DAY_RANGE)

print("All regions processed")