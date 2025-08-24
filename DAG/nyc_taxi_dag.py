from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Import your modules
from etl import extract, transform, load

# Path configs
TAXI_INPUT = os.path.join("input", "yellow_tripdata_2025-01.parquet")
ZONE_INPUT = os.path.join("input", "taxi_zone_lookup.csv")
OUTPUT_PATH = os.path.join("output")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def extract_task(**context):
    spark = extract.get_spark()
    df = extract.extract_taxi_data(spark, TAXI_INPUT)
    zonedf = extract.read_zone_data(spark, ZONE_INPUT)
    # Save temp views so transform can access
    df.createOrReplaceTempView("trips_raw")
    zonedf.createOrReplaceTempView("zone_raw")

def transform_task(**context):
    spark = extract.get_spark()
    df = spark.table("trips_raw")
    zonedf = spark.table("zone_raw")
    df_clean = transform.transform_trips(df)
    paymentsdf = transform.create_payment_dim(spark)
    ratedf = transform.create_rate_dim(spark)
    # Save as temp views for load step
    df_clean.createOrReplaceTempView("trips_clean")
    paymentsdf.createOrReplaceTempView("dim_payment")
    ratedf.createOrReplaceTempView("dim_rate")
    zonedf.createOrReplaceTempView("dim_zone")

def load_task(**context):
    spark = extract.get_spark()
    df_clean = spark.table("trips_clean")
    paymentsdf = spark.table("dim_payment")
    ratedf = spark.table("dim_rate")
    zonedf = spark.table("dim_zone")

    load.load_all_tables(df_clean, paymentsdf, ratedf, zonedf)
    load.output_partition(df_clean, OUTPUT_PATH)

with DAG(
    dag_id="nyc_taxi_etl",
    default_args=default_args,
    description="NYC Taxi ETL with Spark + Postgres",
    schedule_interval="@daily",
    start_date=datetime(2025, 8, 20),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="load",
        python_callable=load_task,
        provide_context=True,
    )

    t1 >> t2 >> t3
