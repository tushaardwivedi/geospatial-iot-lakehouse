import os
import sys
import json
from datetime import datetime
from kafka import KafkaConsumer
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

TOPIC_NAME = 'berlin-fleet-telemetry'

if __name__ == "__main__":
    print("Initializing Cloud Data Lakehouse Infrastructure...")
    
    # Standard static OneLake endpoint
    FABRIC_BASE_URL = "https://onelake.dfs.fabric.microsoft.com"
    
    # Fetch explicit workspace and lakehouse parameters
    workspace_name = os.environ.get("FABRIC_WORKSPACE_NAME")
    lakehouse_name = os.environ.get("FABRIC_LAKEHOUSE_NAME")
    
    if not workspace_name or not lakehouse_name:
        print("CRITICAL CONFIGURATION ERROR: FABRIC_WORKSPACE_NAME or FABRIC_LAKEHOUSE_NAME missing.")
        sys.exit(1)
        
    try:
        # Microsoft Fabric requires item names to include their type suffix when resolved via name paths
        # Format required: YourLakehouseName.Lakehouse (Note the capital 'L')
        if not lakehouse_name.endswith(".Lakehouse") and not lakehouse_name.endswith(".lakehouse"):
            lakehouse_target = f"{lakehouse_name}.Lakehouse"
        else:
            lakehouse_target = lakehouse_name

        # Initialize the token credential from 'az login'
        token_credential = DefaultAzureCredential()
        
        # Connect to the base client service
        service_client = DataLakeServiceClient(account_url=FABRIC_BASE_URL, credential=token_credential)
        
        # In OneLake, the Workspace Name acts as the root container/filesystem folder
        file_system_client = service_client.get_file_system_client(file_system=workspace_name)
        
    except Exception as e:
        print(f"CRITICAL AUTHENTICATION MAPPING FAILURE: {e}")
        sys.exit(1)

    print("Connecting to local Kafka Broker...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        api_version=(3, 4, 0),
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print(f"🎉 Fabric Cloud Consumer active. Streaming data straight to OneLake Landing Zone...")
    print("Press Ctrl+C to stop.")
    
    try:
        for message in consumer:
            payload = message.value
            dt = datetime.fromisoformat(payload.get("timestamp").replace("Z", ""))
            
            # Formulate clean structural path: LakehouseName.Lakehouse/Files/telemetry/year=...
            cloud_directory = f"{lakehouse_target}/Files/telemetry/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
            directory_client = file_system_client.get_directory_client(cloud_directory)
            
            vehicle_id = payload.get("vehicle_id", "unknown_vehicle")
            filename = f"{vehicle_id}_{dt.strftime('%H%M%S%f')}.json"
            json_data = json.dumps(payload)
            
            # Upload data payload directly to cloud target
            file_client = directory_client.get_file_client(filename)
            file_client.upload_data(data=json_data, overwrite=True)
            
            print(f" CLOUD SUCCESS: Uploaded {filename} straight to Fabric OneLake.")
            
    except KeyboardInterrupt:
        print("\nCloud Data Lake Consumer stopped safely.")