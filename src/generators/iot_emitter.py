import time
import json
import random
from datetime import datetime

# Bounding box for Berlin area to keep coordinates realistic
BERLIN_LAT_MIN, BERLIN_LAT_MAX = 52.3400, 52.6600
BERLIN_LON_MIN, BERLIN_LON_MAX = 13.1000, 13.6000

class VehicleSimulator:
    def __init__(self, vehicle_id: str):
        self.vehicle_id = vehicle_id
        # Start at a random location inside Berlin
        self.lat = random.uniform(BERLIN_LAT_MIN, BERLIN_LAT_MAX)
        self.lon = random.uniform(BERLIN_LON_MIN, BERLIN_LON_MAX)
        self.fuel_level = 100.0
        self.speed = 0.0

    def update_position(self):
        """Simulates semi-realistic vehicle movement and telemetry depletion."""
        if self.fuel_level <= 0:
            self.speed = 0.0
            return
        
        # Adjust speed randomly (0 to 80 km/h)
        self.speed = max(0.0, min(80.0, self.speed + random.uniform(-15, 15)))
        
        # Calculate slight coordinate shift based on speed (rough approximation)
        step = (self.speed / 3600) * 0.01  
        self.lat += random.uniform(-step, step)
        self.lon += random.uniform(-step, step)
        
        # Snap back inside boundaries if vehicle wanders out of Berlin
        self.lat = max(BERLIN_LAT_MIN, min(BERLIN_LAT_MAX, self.lat))
        self.lon = max(BERLIN_LON_MIN, min(BERLIN_LON_MAX, self.lon))
        
        # Deplete fuel slowly based on speed
        self.fuel_level -= (self.speed * 0.0005) + 0.01
        self.fuel_level = max(0.0, self.fuel_level)

    def generate_telemetry(self) -> dict:
        self.update_position()
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "vehicle_id": self.vehicle_id,
            "latitude": round(self.lat, 6),
            "longitude": round(self.lon, 6),
            "speed_kmh": round(self.speed, 2),
            "fuel_level_pct": round(self.fuel_level, 2),
            "status": "ACTIVE" if self.fuel_level > 0 else "OUT_OF_FUEL"
        }

if __name__ == "__main__":
    # Simulate a small fleet of 3 delivery vans
    fleet = [VehicleSimulator(f"truck-berlin-{i:03d}") for i in range(1, 4)]
    
    print("Starting IoT Telemetry Streaming Simulation... Press Ctrl+C to stop.")
    try:
        while True:
            for vehicle in fleet:
                data_point = vehicle.generate_telemetry()
                # Print to stdout formatted cleanly as JSON
                print(json.dumps(data_point))
            
            # Emit data every 2 seconds
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nSimulation stopped safely.")