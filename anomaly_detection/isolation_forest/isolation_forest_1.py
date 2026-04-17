import json
import time
import random
import pandas as pd
from collections import deque
from sklearn.ensemble import IsolationForest

# ==========================================
# ⚙️ GENERIC CONFIGURATION
# ==========================================
WINDOW_SIZE = 100        # How many past readings to keep for live training
CONTAMINATION = 0.05     # Expected percentage of anomalies (5%)
POLL_INTERVAL = 2        # Seconds between data polls (for simulation)

# Define which devices to filter and exactly which JSON keys to track.
# You can change these to run on BESS, Meters, or WMS without changing the code below.
DEVICE_FILTER = "solar-inverter" 
FEATURES_TO_TRACK = ['temperature', 'input_power', 'total_power']

# ==========================================
# 🧠 CORE ENGINE
# ==========================================
# Rolling window of flat data to train the model dynamically
data_buffer = deque(maxlen=WINDOW_SIZE)

def extract_dynamic_features(payload):
    """Flattens the JSON dynamically based on the configured tracking list."""
    features = []
    
    for device_id, data in payload.items():
        # Only process devices that match our filter (e.g., all inverters)
        if DEVICE_FILTER in device_id:
            # Dynamically pull only the keys we care about
            row_data = {"device_id": device_id}
            
            for feature in FEATURES_TO_TRACK:
                # Default to 0.0 if a key is missing to prevent crashes
                row_data[feature] = data.get(feature, 0.0)
                
            features.append(row_data)
            
    return features

def run_live_anomaly_detection(current_features):
    """Trains the model on the rolling buffer and predicts the current state."""
    if not current_features:
        return []

    # 1. Add newest data to the rolling buffer
    for row in current_features:
        data_buffer.append(row)
        
    # 2. Wait until we have enough data to establish a baseline (e.g., 50% of window)
    min_required_data = int(WINDOW_SIZE * 0.5)
    if len(data_buffer) < min_required_data:
        print(f"Buffering data... ({len(data_buffer)}/{min_required_data} points collected)")
        return []

    # 3. Prepare the training data (convert to DataFrame)
    df = pd.DataFrame(list(data_buffer))
    
    # Dynamically select only the configured numerical columns for training
    X_train = df[FEATURES_TO_TRACK]
    
    # 4. Initialize and fit the live Isolation Forest
    model = IsolationForest(n_estimators=50, contamination=CONTAMINATION, random_state=42)
    model.fit(X_train)
    
    # 5. Predict anomalies for the current batch of data ONLY
    current_df = pd.DataFrame(current_features)
    X_current = current_df[FEATURES_TO_TRACK]
    
    predictions = model.predict(X_current)
    
    # 6. Gather alerts dynamically
    alerts = []
    for idx, prediction in enumerate(predictions):
        if prediction == -1: # -1 means anomaly detected
            device = current_df.iloc[idx]['device_id']
            
            # Dynamically build the details string based on what we are tracking
            details = ", ".join([f"{f}: {current_df.iloc[idx][f]}" for f in FEATURES_TO_TRACK])
            
            alerts.append({
                "device_id": device,
                "anomaly": "Isolation Forest: Multi-Dimensional Outlier Detected",
                "details": f"Unusual pattern detected -> {details}"
            })
            
    return alerts

# ==========================================
# 🚀 EXECUTION LOOP (SIMULATION)
# ==========================================
if __name__ == "__main__":
    print(f"Starting ML Engine. Tracking {FEATURES_TO_TRACK} on '{DEVICE_FILTER}' devices...")
    
    raw_json_string = """
    {
        "solar-inverter:SP-350K-INH:300007311248200206": {"type": "3ph_inverter", "total_power": 124800.0, "temperature": 48.9, "input_power": 128020.0},
        "solar-inverter:SPI250K-B-H:501501078430m9100014": {"type": "3ph_inverter", "total_power": 110283.0, "temperature": 48.7, "input_power": 113600.0},
        "wms:webdyn:2212514": {"type": "3ph_meter", "ambient_temperature": 29.0, "humidity": 4.4}
    }
    """
    
    base_payload = json.loads(raw_json_string)
    
    # Dynamically find the devices in the payload that match our filter
    active_target_devices = [dev for dev in base_payload.keys() if DEVICE_FILTER in dev]
    
    cycle = 1
    while True:
        print(f"\n--- Live Poll Cycle {cycle} ---")
        
        # At cycle 20, dynamically pick the FIRST available device and spike its data to trigger an anomaly
        if cycle == 20 and active_target_devices:
            test_device = active_target_devices[0]
            base_payload[test_device]["temperature"] = 85.0
            base_payload[test_device]["total_power"] = 20000.0 
        
        # Extract features and run detection
        current_features = extract_dynamic_features(base_payload)
        detected_anomalies = run_live_anomaly_detection(current_features)
        
        if detected_anomalies:
            print("\n🚨 ML MODEL DETECTED ANOMALIES 🚨")
            for alert in detected_anomalies:
                print(f"Device: {alert['device_id']}")
                print(f"Details: {alert['details']}")
            
            # Reset the anomaly to normal so the loop can continue
            if cycle == 20 and active_target_devices:
                test_device = active_target_devices[0]
                base_payload[test_device]["temperature"] = 48.9
                base_payload[test_device]["total_power"] = 124800.0
                
        elif len(data_buffer) >= int(WINDOW_SIZE * 0.5):
            print("Fleet is operating within normal ML parameters.")
            
        cycle += 1
        time.sleep(POLL_INTERVAL)