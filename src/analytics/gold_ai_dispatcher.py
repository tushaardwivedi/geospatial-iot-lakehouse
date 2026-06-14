import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# FIX: Using the correct, modern and updated SDK library mapping
from google import genai
from google.genai import types

SILVER_DIR = '/workspaces/geospatial-iot-lakehouse/data/silver/telemetry_parquet/'

def get_critical_anomalies():
    """Uses PySpark to filter and isolate ONLY high-risk events (Token Efficiency)."""
    if not os.path.exists(SILVER_DIR):
        print(f"Error: Silver data missing at {SILVER_DIR}")
        return []

    spark = SparkSession.builder \
        .appName("SilverToGoldAnomalies") \
        .config("spark.master", "local[*]") \
        .getOrCreate()

    try:
        df = spark.read.parquet(SILVER_DIR)
        
        # Filter rules: Speeding or dangerously low fuel levels
        anomaly_df = df.filter(
            (col("speed_kmh") > 75.0) | 
            (col("fuel_level_pct") < 10.0)
        )
        
        essential_data = anomaly_df.select(
            "vehicle_id", "speed_kmh", "fuel_level_pct", "status"
        ).distinct().collect()
        
        return [row.asDict() for row in essential_data]
    except Exception as e:
        print(f"Spark error during filtering: {e}")
        return []
    finally:
        spark.stop()

def dispatch_ai_recovery(anomaly_list):
    """Sends minified data to Gemini 3.5 Flash using the modern unified SDK."""
    if not anomaly_list:
        print("No critical fleet anomalies detected by Spark. Zero tokens used.")
        return

    # Check for the correct environment variable
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("CRITICAL SECURITY ERROR: GEMINI_API_KEY environment variable not detected.")
        sys.exit(1)

    # Initialize the modern unified client wrapper (automatically captures GEMINI_API_KEY)
    client = genai.Client()

    # Define strict behavior constraints inside the updated GenerateContentConfig layout
    config = types.GenerateContentConfig(
        system_instruction=(
            "You are an automated logistics dispatch agent. Analyze the provided fleet anomalies. "
            "Output a minified JSON object mapping each vehicle_id to a specific dispatch_action. "
            "Do not include markdown blocks, text explanations, or conversational filler. Output raw JSON only."
        )
    )

    prompt = f"Anomalies: {anomaly_list}"
    
    print(f"Sending {len(anomaly_list)} isolated cases to Gemini Engine...")
    
    # Execute call utilizing Gemini 3.5 Flash
    response = client.models.generate_content(
        model='gemini-3.5-flash',
        contents=prompt,
        config=config
    )
    
    print("\n--- GOLD LAYER AI DISPATCH PLAN (RAW JSON) ---")
    print(response.text.strip())
    print("----------------------------------------------")

if __name__ == "__main__":
    print("Starting Gold Layer Analytics & AI Dispatcher...")
    critical_cases = get_critical_anomalies()
    dispatch_ai_recovery(critical_cases)