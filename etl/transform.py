# etl/transform.py
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def transform_trips(df: DataFrame) -> DataFrame:
    """
    Clean and transform the NYC Taxi trip dataframe.

    Steps:
    1. Calculate trip duration in minutes.
    2. Drop irrelevant columns.
    3. Add binary flag for tip status (1 = tip paid, 0 = no tip).
    4. Drop rows with too many null values (allow up to 3 nulls per row).

    Args:
        df (DataFrame): Raw taxi trips DataFrame.

    Returns:
        DataFrame: Transformed DataFrame with additional features.
    """
    df = df.withColumn(
        "journey_minutes",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
    )

    df = df.drop("store_and_fwd_flag")

    df = df.withColumn(
        "tip_status",
        F.when(F.col("tip_amount") > 0, F.lit(1)).otherwise(F.lit(0))
    )

    df = df.dropna(thresh=len(df.columns) - 3)

    return df


def create_payment_dim(spark: SparkSession) -> DataFrame:
    """
    Create a payment method dimension table.

    Args:
        spark (SparkSession): Active Spark session.

    Returns:
        DataFrame: Payment dimension DataFrame.
    """
    data = [
        (1, 'Credit card'),
        (2, 'Cash'),
        (3, 'No charge'),
        (4, 'Dispute'),
        (5, 'Unknown'),
        (6, 'Voided'),
    ]
    return spark.createDataFrame(data, ['payment_id', 'payment_method'])


def create_rate_dim(spark: SparkSession) -> DataFrame:
    """
    Create a rate type dimension table.

    Args:
        spark (SparkSession): Active Spark session.

    Returns:
        DataFrame: Rate dimension DataFrame.
    """
    data = [
        (1, "Standard rate"),
        (2, "JFK"),
        (3, "Newark"),
        (4, "Nassau or Westchester"),
        (5, "Negotiated fare"),
        (6, "Group ride")
    ]
    return spark.createDataFrame(data, ["rate_id", "rate_type"])


def load_zone_dim(spark: SparkSession, df) -> DataFrame:
    """
    Load and clean taxi zone lookup data.

    Args:
        spark (SparkSession): Active Spark session.
        path (str): Path to taxi_zone_lookup.csv file.

    Returns:
        DataFrame: Zone dimension DataFrame with lowercase column names.
    """
    return df.toDF(*[c.lower() for c in df.columns])
