"""
Automating the entire workflow of the Fire-Weather data pipeline
1. Set up the necessary Docker containers (minIO, Spark, PostgreSQL)
2. Test and prepare object storage in MinIO using boto3
3. Launching ingestion scrips for NASA FIRMS API and Open-Meteo API
4. Distributed PySpark ETL data processing pipeline
5. Perform a final record count check in the analytical PostgreSQL database
"""

import os
import subprocess
import sys
import time
import boto3

# Configuration & constants
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "cariappa"
MINIO_SECRET_KEY = "D3chamma"
BUCKET_NAME = "fire-weather-ingestion"


def print_header(title):  #Printing a formatted ASCII header for tracking pipeline progress in the cmd output logs
    
    print(f" {title.upper()}")
    
def run_command(command, step_description): #Executing a shell command to track execution time and exit status 
    print_header(step_description)
    start_time = time.time()
    
    result = subprocess.run(command, shell=True) #executing system command by the subprocess
    
    if result.returncode != 0:   #stopping pipeline execution if any step returns a non zero exit code
        print(f"\n ERROR: Step '{step_description}' failed with exit code {result.returncode}")
        sys.exit(1)
        
    elapsed = round(time.time() - start_time, 2)
    print(f" SUCCESS: {step_description} completed in {elapsed}s")


def ensure_minio_bucket():
    #Verify and automatically create the required MinIO bucket if missing
    print_header("Verifying MinIO Storage Bucket")
    s3 = boto3.client(              #initializing S3 client that is targeted at the minIO endpoint
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    
    buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]     # retrieve list of existing buckets
    if BUCKET_NAME not in buckets:
        print(f"Bucket '{BUCKET_NAME}' not found. Creating bucket")
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f" Bucket '{BUCKET_NAME}' created successfully.")
    else:
        print(f" Bucket '{BUCKET_NAME}' already exists and is ready.")


def main():     #main orchestration function that drives the whole workflow
    print_header("Starting Automated Fire-Weather Pipeline Orchestration")

    # Start Docker Containers (MinIO, Spark, Postgres)
    run_command("docker compose up -d", "Spinning Up Docker Infrastructure")
    
    # brief pause to allow PostgreSQL and MinIO to accept network connections
    time.sleep(5)

    # Check/Create MinIO Bucket
    ensure_minio_bucket()

    # Fetch FIRMS Data to MinIO
    run_command(f'"{sys.executable}" scripts/fetch_firms_loop.py', "Executing FIRMS Ingestion Script")

    # Fetch Open-Meteo Weather Data to MinIO
    run_command(f'"{sys.executable}" scripts/fetch_openmeteo_loop.py', "Executing Open-Meteo Weather Ingestion Script")

    # Run PySpark Distributed Pipeline
    run_command(f'"{sys.executable}" scripts/spark_pipeline.py', "Executing Distributed PySpark Processing Pipeline")

    # Verify Results in PostgreSQL Database (final joined db count)
    verify_pg_cmd = (
        'docker exec -i fire-weather-pipeline-postgres-1 '
        'psql -U cariappa -d fireweather -c '
        '"SELECT region, COUNT(*) FROM fire_weather_joined GROUP BY region;"'
    )
    run_command(verify_pg_cmd, "Verifying Joined Results in PostgreSQL")

    print_header("Pipeline Execution Completed")


if __name__ == "__main__":
    main()