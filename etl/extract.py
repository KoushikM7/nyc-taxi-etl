# etl/extract.py
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv()


def get_spark():
    """Create or retrieve Spark session with Postgres driver."""
    jar_path = os.getenv("POSTGRES_JAR", os.environ["POSTGRES_JAR"])
    return (
        SparkSession.builder
        .appName("NYC-Taxi-ETL")
        .config("spark.jars", jar_path)
        .getOrCreate()
    )

def extract_taxi_data(spark, input_path: str):
    """Read taxi parquet file."""
    return spark.read.parquet(input_path)

def read_zone_data(spark, file_path: str):
    """Read taxi zone lookup CSV file and lowercase column names."""
    df = spark.read.csv(file_path, inferSchema=True, header=True)
    df = df.toDF(*[c.lower() for c in df.columns])
    return df
