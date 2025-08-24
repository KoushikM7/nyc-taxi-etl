# main.py
import os
from etl import extract, transform, load

def main():
    # Create Spark session

    os.environ["PYSPARK_PYTHON"] = r"C:\Users\Goku\AppData\Local\Programs\Python\Python310\python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Goku\AppData\Local\Programs\Python\Python310\python.exe"

    spark = extract.get_spark()

    # -------------------------
    # 1. Extract
    # -------------------------
    taxi_input = os.path.join("input", "yellow_tripdata_2025-01.parquet")
    zone_input = os.path.join("input", "taxi_zone_lookup.csv")
    output_path = os.path.join("output")
    df = extract.extract_taxi_data(spark, taxi_input)
    zonedf = extract.read_zone_data(spark, zone_input)

    # -------------------------
    # 2. Transform
    # -------------------------
    df_clean = transform.transform_trips(
        df
    )
    paymentsdf = transform.create_payment_dim(spark)
    ratedf = transform.create_rate_dim(spark)


    # -------------------------
    # 3. Load to PostgreSQL
    # -------------------------
    load.load_all_tables(df_clean, paymentsdf, ratedf, zonedf)

    # -------------------------
    # 4. Load to disk, partitioned by date
    # -------------------------
    load.output_partition(df_clean,output_path)

    print("✅ ETL Pipeline finished successfully!")

if __name__ == "__main__":
    main()
