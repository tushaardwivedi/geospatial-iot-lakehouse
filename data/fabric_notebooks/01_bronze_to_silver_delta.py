# ==============================================================================
# Microsoft Fabric Synapse Notebook: Bronze to Silver Delta Compaction
# Core Focus: Schema Enforcement, Columnar Optimization, & ACID Transactions
# ==============================================================================

from pyspark.sql.functions import col, to_timestamp

# 1. Define direct local OneLake relative paths
BRONZE_JSON_PATH = "Files/telemetry/year=*/month=*/day=*/*.json"
SILVER_TABLE_NAME = "telemetry_silver"

print("⚡ Starting Cloud PySpark Silver Compaction Engine...")

try:
    # 2. Read the raw, unstructured JSON files sitting in the Bronze Landing Zone
    print(f"Reading raw stream packets from: {BRONZE_JSON_PATH}")
    raw_df = spark.read.json(BRONZE_JSON_PATH)
    
    # 3. Data Cleaning & Schema Transformation
    # Enforce correct structural data types and convert the ISO string timestamp to a formal timestamp object
    cleaned_df = raw_df.withColumn("speed_kmh", col("speed_kmh").cast("double")) \
                       .withColumn("fuel_level_pct", col("fuel_level_pct").cast("double")) \
                       .withColumn("timestamp", to_timestamp(col("timestamp"))) \
                       .dropDuplicates(["vehicle_id", "timestamp"]) # Eliminate network retry duplicates
    
    print(f"Identified and processed {cleaned_df.count()} unique telemetry events.")
    
    # 4. Write the optimized dataset directly into the Tables layer as a Delta Table
    print(f"Writing optimized data to Delta Table: {SILVER_TABLE_NAME}")
    cleaned_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(SILVER_TABLE_NAME)
        
    print("🎉 SUCCESS: Silver Delta Layer updated and optimized successfully!")

except Exception as e:
    print(f"❌ CRITICAL PIPELINE FAILURE: {e}")


# In[6]:


# Cell 2: Inline Cluster Package Management
# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install google-genai


# In[7]:


# Cell 1: Secure Environment Handshake
import os

# Inject your key directly into the Spark driver's active operating system memory
os.environ["GEMINI_API_KEY"] = "PLACEHOLDER_ENTERPRISE_KEY_DO_NOT_COMMIT"
print("🔒 API Credentials injected safely into the Cloud Spark Environment.")


# In[13]:


# Cell 3: Gold Layer Analytical Filtering & AI Dispatch Orchestration (Resilient Cloud Passing)
from pyspark.sql.functions import col
from google import genai
from google.genai import types
from google.genai import errors
import os
import sys

print("🤖 Accessing Silver Delta Layer for High-Risk Diagnostics...")

try:
    # 1. Fetch the key from the driver's environment using Python
    api_key_check = os.environ.get("GEMINI_API_KEY")
    if not api_key_check:
        print("❌ CRITICAL ERROR: GEMINI_API_KEY not found in active notebook memory.")
        raise ValueError("Missing GEMINI_API_KEY configuration.")

    # 2. Read directly from the managed Silver Delta Table
    silver_df = spark.read.table("telemetry_silver")
    
    # 3. Token Efficiency Filter: Isolate only extreme speed or low fuel anomalies
    anomaly_df = silver_df.filter(
        (col("speed_kmh") > 75.0) | 
        (col("fuel_level_pct") < 10.0)
    )
    
    essential_data = anomaly_df.select(
        "vehicle_id", "speed_kmh", "fuel_level_pct", "status"
    ).distinct().collect()
    
    anomalies = [row.asDict() for row in essential_data]
    
    if not anomalies:
        print("✅ No critical fleet anomalies detected in the Delta Table. Zero tokens used.")
    else:
        print(f"⚠️ Isolated {len(anomalies)} critical cases. Initializing Gemini 3.5 Flash Client...")
        
        # 4. FIX: Pass the API key EXPLICITLY into the client initialization parameter
        client = genai.Client(api_key=api_key_check)
        
        # 5. Enforce strict behavioral and structural JSON constraints
        config = types.GenerateContentConfig(
            system_instruction=(
                "You are an automated logistics dispatch agent. Analyze the provided fleet anomalies. "
                "Output a minified JSON object mapping each vehicle_id to a specific dispatch_action. "
                "Do not include markdown code blocks, text explanations, or conversational filler. Output raw JSON only."
            )
        )
        
        prompt = f"Anomalies: {anomalies}"
        
        try:
            response = client.models.generate_content(
                model='gemini-3.5-flash',
                contents=prompt,
                config=config
            )
            
            print("\n========================================================")
            print("🎉 GOLD LAYER GENERATED AI DISPATCH PLAN (RAW JSON)")
            print("========================================================")
            print(response.text.strip())
            print("========================================================")
            
        except errors.APIError as api_err:
            print(f"❌ [API ERROR] Cloud gateway communication failure: {api_err}")
            
except Exception as e:
    print(f"❌ [PIPELINE ERROR] Gold orchestration crashed: {e}")

