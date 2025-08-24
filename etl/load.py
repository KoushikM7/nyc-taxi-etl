# etl/load.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,to_date
from dotenv import load_dotenv
import os

load_dotenv()

def load_to_postgres(df: DataFrame, table_name: str, url: str, properties: dict, mode: str = "overwrite") -> None:
    """
    Load a Spark DataFrame into a PostgreSQL table.

    Args:
        df (DataFrame): Spark DataFrame to be written.
        table_name (str): Target PostgreSQL table name.
        url (str): JDBC URL for PostgreSQL (e.g., jdbc:postgresql://localhost:5432/dbname).
        properties (dict): Connection properties (user, password, driver).
        mode (str): Save mode - "overwrite", "append", "ignore", "error". Default = "overwrite".
    """
    df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)
    


def load_all_tables(fact_df: DataFrame, payment_df: DataFrame, rate_df: DataFrame, zone_df: DataFrame) -> None:
    """
    Load fact and dimension tables into PostgreSQL.

    Args:
        fact_df (DataFrame): Transformed trips fact DataFrame.
        payment_df (DataFrame): Payment dimension DataFrame.
        rate_df (DataFrame): Rate dimension DataFrame.
        zone_df (DataFrame): Zone dimension DataFrame.
    """
    postgres_url = os.environ["POSTGRES_URL"]
    properties = {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver"
    }

    load_to_postgres(fact_df, "fact_trips", postgres_url, properties)
    load_to_postgres(payment_df, "dim_payment", postgres_url, properties)
    load_to_postgres(rate_df, "dim_rate", postgres_url, properties)
    load_to_postgres(zone_df, "dim_zone", postgres_url, properties)


def output_partition(df : DataFrame,output_path : str):
    df = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
    df.write.partitionBy("pickup_date").parquet(output_path,mode="overwrite")