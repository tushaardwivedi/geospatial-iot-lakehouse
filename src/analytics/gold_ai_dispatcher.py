import os
import sys
import time  # For managing retry intervals
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from google import genai
from google.genai import types
from google.genai import errors  # FIX: Import API errors framework

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
    """Sends minified data to Gemini 3.5 Flash with built-in resilient retry logic."""
    if not anomaly_list:
        print("No critical fleet anomalies detected by Spark. Zero tokens used.")
        return

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("CRITICAL SECURITY ERROR: GEMINI_API_KEY environment variable not detected.")
        sys.exit(1)

    client = genai.Client()

    config = types.GenerateContentConfig(
        system_instruction=(
            "You are an automated logistics dispatch agent. Analyze the provided fleet anomalies. "
            "Output a minified JSON object mapping each vehicle_id to a specific dispatch_action. "
            "Do not include markdown blocks, text explanations, or conversational filler. Output raw JSON only."
        )
    )

    prompt = f"Anomalies: {anomaly_list}"
    
    # ADVANCED RESILIENCE: Retry Configuration Parameters
    max_retries = 3
    initial_delay = 4  # Seconds to wait on first failure
    
    print(f"Sending {len(anomaly_list)} isolated cases to Gemini Engine...")
    
    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model='gemini-3.5-flash',
                contents=prompt,
                config=config
            )
            
            # If successful, print results and exit the retry loop entirely
            print("\n--- GOLD LAYER AI DISPATCH PLAN (RAW JSON) ---")
            print(response.text.strip())
            print("----------------------------------------------")
            return
            
        except errors.APIError as e:
            # Check if it's a temporary high demand (503) or rate limit error
            if attempt < max_retries - 1:
                print(f"⚠️ [API WARNING] Google servers busy (Status {e.code}). Retrying in {initial_delay}s... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(initial_delay)
                initial_delay *= 2  # Exponential backoff: 4s -> 8s -> 16s
            else:
                print("❌ [CRITICAL ERROR] Gemini servers down after maximum retry attempts.")
                raise e # Re-raise the error to let the orchestrator fail-fast safely

if __name__ == "__main__":
    print("Starting Gold Layer Analytics & AI Dispatcher...")
    critical_cases = get_critical_anomalies()
    dispatch_ai_recovery(critical_cases)