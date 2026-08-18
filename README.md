# fire-weather-pipeline
Multi-Regional Wildfire and Meteorological Analytics:  A Distributed PySpark, MinIO &amp; PostgreSQL Pipeline

PROJECT READ ME: SCALABLE MULTI-REGIONAL WILDFIRE & METEOROLOGICAL PIPELINE


1. Repository:

Scripts/
run_pipeline.py            # Master automation orchestrator script
analyze_results.py         # Follow-up statistical & ML visualization script
docker-compose.yml         # Container definitions (MinIO, Spark, Postgres)
readme.txt                 # Project instructions and documentation
fetch_firms_loop.py         # NASA FIRMS API extraction script -> MinIO
fetch_openmeteo_loop.py       # Open-Meteo API extraction script -> MinIO
spark_pipeline.py      # Distributed PySpark ETL & JDBC PostgreSQL script

figures/                   		# Output folder containing generated report figures
analysis1_correlation_matrix.png
analysis2_regional_frp_comparison.png
analysis3_feature_importance.png

minio-data/                # Local persistent volume mount for MinIO storage


2. Instructions & requirements:

Python 3.10+ installed
Docker Desktop running 

a) Unzip the submission archive
Extract the zip file and navigate to root directory
  $ cd fire-weather-pipeline

b) Create and activate Python virtual environment on Windows PowerShell
  $ python -m venv venv
  $ .\venv\Scripts\Activate.ps1

c) Install required dependencies
pyspark==3.5.9
boto3
requests
pandas
matplotlib
seaborn
scikit-learn
psycopg2-binary
sqlalchemy

d) Executing the pipeline
The entire pipeline is fully automated and idempotent.
Run the single-command orchestrator to spin up infrastructure, ingest raw sources 
to MinIO, execute distributed PySpark spatial grid joins, and load PostgreSQL

    $ python run_pipeline.py

e) Executing the analyses
To extract joined data from PostgreSQL, compute correlation matrices, regional 
FRP severity comparisons, and Random Forest feature importances

    $ python analyze_results.py

This script exports three high-resolution charts into the figures/ folder.



3. Expected Results
The final PostgreSQL database verification stage will display the exact row counts below

   region        | count
  ---------------+-------
   mediterranean |    19
   se_australia  |   287
   california    |   294
  (3 rows)

Total joined records in database = 600 records.

NOTE: Video presentation has been uploaded to moodle and the youtube link is at the end of the report
