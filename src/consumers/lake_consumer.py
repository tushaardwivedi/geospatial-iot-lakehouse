import os
import json
from datetime import datetime
from kafka import KafkaConsumer

TOPIC_NAME = 'berlin-fleet-telemetry'
DATA_LAKE_BRONZE = '/workspaces/geospatial-iot-lakehouse/data/bronze/telemetry'

if __name__ == "__main__":
    print("Initializing Data Lake Consumer connection to Kafka...")
    
    # Initialize Kafka Consumer using the pure-Python driver matching the producer
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',  # Consume every message in the broker's log history
        enable_auto_commit=True,       # Acknowledge to Kafka that messages are read
        api_version=(3, 4, 0),
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print(f"Data Lake Consumer active, listening on Kafka Topic: '{TOPIC_NAME}'...")
    print("Writing files partitioned by arrival date. Press Ctrl+C to stop.")
    
    try:
        for message in consumer:
            payload = message.value
            
            # Extract time metrics for spatial partitions
            ts_str = payload.get("timestamp", datetime.utcnow().isoformat())
            dt = datetime.fromisoformat(ts_str.replace("Z", ""))
            
            # Formulate the partitioned directory structure: year=YYYY/month=MM/day=DD
            partition_path = os.path.join(
                DATA_LAKE_BRONZE, 
                f"year={dt.year}", 
                f"month={dt.month:02d}", 
                f"day={dt.day:02d}"
            )
            
            # Create local folder pathways recursively if they don't exist yet on disk
            os.makedirs(partition_path, exist_ok=True)
            
            # Create a completely unique filename using vehicle id and precise microsecond timestamps
            vehicle_id = payload.get("vehicle_id", "unknown_vehicle")
            filename = f"{vehicle_id}_{dt.strftime('%H%M%S%f')}.json"
            file_fullpath = os.path.join(partition_path, filename)
            
            with open(file_fullpath, 'w') as f:
                json.dump(payload, f)
                
            print(f" SUCCESS: Persisted raw log to Data Lake: {filename}")
            
    except KeyboardInterrupt:
        print("\nData Lake Consumer stopped safely.")