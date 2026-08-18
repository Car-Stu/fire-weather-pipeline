"""
Description:
The script fetches historical daily weather data from the Open-Meteo API
for the coordinates of the region's centroid.
It then uploads the raw, unnested JSON responses to MinIO Object Storage.
"""

import requests
import boto3

#Centroid coordinates and meteorological metrics
REGIONS = {
    "california": {"lat": 36.78, "lon": -119.42},
    "mediterranean": {"lat": 39.0, "lon": 22.0},
    "se_australia": {"lat": -35.0, "lon": 147.0},
}

START_DATE = "2013-01-01"
END_DATE = "2025-03-31"
# Required weather variables for fire risk modeling
DAILY_VARS = "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max,relative_humidity_2m_mean"

#Initialize S3 client for MinIO object storage
client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="cariappa",
    aws_secret_access_key="D3chamma",
)

#Data ingestion loop
for region_name, coords in REGIONS.items():
    #Construct Open-Meteo Archive API endpoint URL
    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={coords['lat']}&longitude={coords['lon']}"
        f"&start_date={START_DATE}&end_date={END_DATE}"
        f"&daily={DAILY_VARS}"
        f"&timezone=auto"
    )
    #Requesting weather archive JSON
    response = requests.get(url)
    print(region_name, response.status_code)

    #Log warning if payload signals API error
    if "error" in response.text[:50].lower():
        print(f"  WARNING: {region_name} response looks like an error:")
        print(f"  {response.text[:300]}")

    #Save JSON archive directly into MinIO storage
    try:
        client.put_object(
            Bucket="fire-weather-ingestion",
            Key=f"weather/{region_name}/{START_DATE}_to_{END_DATE}.json",
            Body=response.text,
        )
        print(f"  {region_name} uploaded")
    except Exception as e:
        print(f"  {region_name} upload Failed: {e}")

print("All regions processed")


client.put_object(
        Bucket="fire-weather-ingestion",
        Key=f"weather/{region_name}/{START_DATE}_to_{END_DATE}.json",
        Body=response.text,
    )

print("All regions uploaded")