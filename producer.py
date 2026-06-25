import json
import random
import time
from confluent_kafka import Producer

# --- Confluent Cloud Configuration ---
BOOTSTRAP_SERVER = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
API_KEY = "VB7WNJFBRDYQI3OJ"
API_SECRET = "cfltkOmJUL4A5HLU9ocOzDTHT2YQtUgUWXDKxzQZckodauWnuOBFW6CO0IL6d1QQ"
TOPIC_NAME = "vehicle-telemetry"  # Change this to your actual Confluent topic name

conf = {
    'bootstrap.servers': BOOTSTRAP_SERVER,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': API_KEY,
    'sasl.password': API_SECRET,
    'client.id': 'obd-mock-service-cloud',
    'acks': 'all'
}

# Initialize the producer with Confluent cloud settings
producer = Producer(conf)

# --- Mock Data State ---
vehicle_state = {
    "device_id": "jio-obd-98765",
    "vin": "1HGCR2F8XHA000000",
    "lat": 12.9716, 
    "lon": 77.5946,
    "speed": 50,
    "ignition": True
}

def delivery_report(err, msg):
    """ Callback to verify cloud delivery acknowledgment """
    if err is not None:
        print(f"❌ Cloud delivery failed: {err}")
    else:
        print(f"✅ Sent to Confluent Cloud -> Topic: {msg.topic()} | Partition: [{msg.partition()}] | Offset: {msg.offset()}")

def generate_mock_telemetry(state):
    """ Generates realistic fluctuating OBD/GPS data streams """
    if state["ignition"]:
        state["speed"] = max(10, int(state["speed"] + random.choice([-4, -1, 0, 1, 4, 6])))
        if state["speed"] > 110: state["speed"] = 90
        
        rpm = int(state["speed"] * 33 + random.randint(-150, 150))
        coolant_temp = random.randint(91, 95)
        engine_load = round(random.uniform(35.0, 75.0), 1)
        
        state["lat"] += random.uniform(-0.0004, 0.0004)
        state["lon"] += random.uniform(-0.0004, 0.0004)
    else:
        state["speed"] = 0
        rpm = 0
        coolant_temp = 40
        engine_load = 0.0

    dtc_list = []
    if random.random() < 0.01: # 1% chance of error code
        dtc_list = [random.choice(["P0300", "P0171"])]

    return {
        "device_id": state["device_id"],
        "vin": state["vin"],
        "timestamp": int(time.time() * 1000),
        "ignition_status": state["ignition"],
        "metrics": {
            "engine_rpm": rpm,
            "vehicle_speed_kmh": state["speed"],
            "coolant_temp_celsius": coolant_temp,
            "engine_load_percentage": engine_load,
            "fuel_level_percentage": round(random.uniform(54.0, 54.3), 2),
            "battery_voltage": round(random.uniform(13.9, 14.1), 1),
            "mass_air_flow_g_s": round(state["speed"] * 0.32, 2)
        },
        "location": {
            "latitude": round(state["lat"], 6),
            "longitude": round(state["lon"], 6),
            "gps_speed_kmh": float(state["speed"]),
            "heading_degrees": random.choice([0, 90, 180, 270])
        },
        "accelerometer": {
            "x_axis": round(random.uniform(-0.1, 0.1), 2),
            "y_axis": round(random.uniform(-0.2, 0.2), 2),
            "z_axis": round(random.uniform(0.9, 1.0), 2)
        },
        "diagnostics": {
            "dtc_count": len(dtc_list),
            "active_dtcs": dtc_list
        }
    }

# --- Execution ---
print("Connecting to Confluent Cloud and streaming OBD payloads... (Ctrl+C to stop)")
try:
    while True:
        telemetry_data = generate_mock_telemetry(vehicle_state)
        json_payload = json.dumps(telemetry_data)
        message_key = vehicle_state["vin"]
        
        # Async stream to Cloud broker
        producer.produce(
            topic=TOPIC_NAME, 
            key=message_key, 
            value=json_payload, 
            callback=delivery_report
        )
        
        producer.poll(0) # Polls events queue to trigger callbacks
        time.sleep(2)     # Push data every 2 seconds

except KeyboardInterrupt:
    print("\nStopping Cloud Producer...")
finally:
    print("Flushing pending messages...")
    producer.flush()
    print("Disconnected safely.")