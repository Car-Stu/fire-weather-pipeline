"""
Description:
Reads raw CSV active fire records and JSON weather payloads from MinIO.
Performs grid-snapping (0.25° grid cells) of vector coordinates, unnesting
and spatial-temporal inner-joining and finally appending the resulting
records to PostgreSQL in an idempotent dynamic write mode design pattern.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    arrays_zip,
    col,
    count,
    explode,
    lit,
    round as spark_round,
    sum as spark_sum,
)

# Initializing Spark Session with S3A and JDBC postgres drivers
spark = (
    SparkSession.builder.appName("FireWeatherPipeline")
    # S3A Object Storage connection configuration for MinIO
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "cariappa")
    .config("spark.hadoop.fs.s3a.secret.key", "D3chamma")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config(
        "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    # Download and load required Maven packages (Hadoop AWS SDK & PostgreSQL JDBC)
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3",
    )
    .getOrCreate()
)

#List of target regions matching the MinIO directory structure
REGIONS = ["california", "mediterranean", "se_australia"]

# Pipeline Execution Loop (using enumerate to get idx)
for idx, region_name in enumerate(REGIONS):
    print(f"       Processing Region: {region_name.upper()}")
    
    # Read & Aggregate FIRMS Data
    firms_path = f"s3a://fire-weather-ingestion/firms/{region_name}/*.csv"
    print(f"Reading FIRMS data from: {firms_path}")

    firms_df = spark.read.csv(firms_path, header=True)
    # Spatial coordinate discretization formula: round(coord * 4) / 4 which gives 0.25 degree grid
    firms_agg = (
        firms_df.withColumn(
            "grid_lat", spark_round(col("latitude").cast("double") * 4) / 4
        )
        .withColumn(
            "grid_lon", spark_round(col("longitude").cast("double") * 4) / 4
        )
        .withColumn("frp", col("frp").cast("double"))
        .groupBy("grid_lat", "grid_lon", "acq_date")
        .agg(
            count("*").alias("detection_count"),
            spark_sum("frp").alias("total_frp"),
        )
    )

    #Read & Explode Weather Data (unnesting parallel weather JSON arrays)
    weather_path = f"s3a://fire-weather-ingestion/weather/{region_name}/*.json"
    print(f"Reading Weather data from: {weather_path}")

    weather_df = spark.read.option("multiLine", "true").json(weather_path)

    weather_flat = (
        weather_df.select(
            col("latitude"),
            col("longitude"),
            explode(
                arrays_zip(
                    col("daily.time"),
                    col("daily.temperature_2m_max"),
                    col("daily.temperature_2m_min"),
                    col("daily.precipitation_sum"),
                    col("daily.windspeed_10m_max"),
                    col("daily.relative_humidity_2m_mean"),
                )
            ).alias("daily_row"),
        )
        .select(
            col("latitude"),
            col("longitude"),
            col("daily_row.time").alias("date"),
            col("daily_row.temperature_2m_max").alias("temp_max"),
            col("daily_row.temperature_2m_min").alias("temp_min"),
            col("daily_row.precipitation_sum").alias("precipitation"),
            col("daily_row.windspeed_10m_max").alias("wind_max"),
            col("daily_row.relative_humidity_2m_mean").alias("humidity"),
        )
        #Snap weather coordinate centroids to the same 0.25 degree spatial grid
        .withColumn("grid_lat", spark_round(col("latitude") * 4) / 4)
        .withColumn("grid_lon", spark_round(col("longitude") * 4) / 4)
    )

    #Join Fire and Weather Data
    joined = (
        firms_agg.join(
            weather_flat,
            on=[
                firms_agg.grid_lat == weather_flat.grid_lat,
                firms_agg.grid_lon == weather_flat.grid_lon,
                firms_agg.acq_date == weather_flat.date,
            ],
            how="inner",
        )
        .select(
            firms_agg.grid_lat,
            firms_agg.grid_lon,
            firms_agg.acq_date.alias("date"),
            "detection_count",
            "total_frp",
            "temp_max",
            "temp_min",
            "precipitation",
            "wind_max",
            "humidity",
        )
        .withColumn("region", lit(region_name))
    )

    joined_count = joined.count()
    print(f"Joined rows for {region_name}: {joined_count}")

    #Write Region Batch to Postgres
    # Overwrite old records on 1st region (idx == 0), then append remaining regions
    write_mode = "overwrite" if idx == 0 else "append"
    print(f"Writing to Postgres using mode: '{write_mode}'")

    joined.write.format("jdbc").option(
        "url", "jdbc:postgresql://localhost:5433/fireweather"
    ).option("dbtable", "fire_weather_joined").option(
        "user", "cariappa"
    ).option(
        "password", "D3chamma"
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        write_mode
    ).save()

    print(f"Successfully saved {region_name} to Postgres")

print("\nAll regions processed")