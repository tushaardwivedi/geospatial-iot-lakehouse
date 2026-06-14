import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, to_timestamp

# Hard directory targets
BRONZE_DIR = '/workspaces/geospatial-iot-lakehouse/data/bronze/telemetry/'
SILVER_DIR = '/workspaces/geospatial-iot-lakehouse/data/silver/telemetry_parquet/'

# Strict Data Schema to prevent malicious/corrupt injections
iot_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed_kmh", DoubleType(), True),
    StructField("fuel_level_pct", DoubleType(), True),
    StructField("status", StringType(), True)
])

def initialize_spark():
    """Builds a pure, dependency-free local Spark Session."""
    return SparkSession.builder \
        .appName("BronzeToSilverCompaction") \
        .config("spark.master", "local[*]") \
        .getOrCreate()

def run_compaction_pipeline():
    if not os.path.exists(BRONZE_DIR):
        print(f"Error: No raw Bronze data discovered at {BRONZE_DIR}.")
        return
        
    spark = initialize_spark()
    print(f"PySpark Engine active. Scanning for raw files inside: {BRONZE_DIR}")

    try:
        # Read the raw time-partitioned JSON files recursively
        raw_df = spark.read \
            .schema(iot_schema) \
            .json(os.path.join(BRONZE_DIR, "year=*/*/*/*.json"))
        
        row_count = raw_df.count()
        print(f"SUCCESS: Loaded raw records from Bronze Lake. Total count: {row_count}")
        
        if row_count == 0:
            print("Bronze directory is present but contains empty records.")
            return
            
        # Data Cleansing and Transformation
        processed_df = raw_df \
            .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
            .drop("timestamp") \
            .filter(col("vehicle_id").isNotNull())
            
        print(f"Writing optimized, compacted data to Silver Layer: {SILVER_DIR}")
        
        # Write out to standard high-performance column-oriented Parquet format
        processed_df.write \
            .mode("overwrite") \
            .parquet(SILVER_DIR)
            
        print("🎉 Silver Layer processing execution complete. Small files compacted successfully.")
        
    except Exception as e:
        print(f"An error occurred during transformation execution: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_compaction_pipeline()