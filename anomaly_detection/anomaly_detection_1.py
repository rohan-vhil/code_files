import collections
import control.control_base as ctrl
from control import control_base as ctrl

# --- STATEFUL MEMORY ---
# Increased buffer size to 10 for a more stable trend line
HISTORY_SIZE = 10
device_history = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.deque(maxlen=HISTORY_SIZE)))

def predict_time_to_threshold(y_values, threshold, is_increasing_fault=True):
    """
    Calculates the slope of the data and predicts how many intervals 
    until it hits the critical threshold.
    """
    n = len(y_values)
    if n < int(HISTORY_SIZE * 0.8): # Need buffer to be at least 80% full to predict
        return None, 0

    x_values = list(range(n))
    mean_x = sum(x_values) / n
    mean_y = sum(y_values) / n

    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(x_values, y_values))
    denominator = sum((x - mean_x) ** 2 for x in x_values)
    
    if denominator == 0:
        return None, 0
        
    slope = numerator / denominator
    current_val = y_values[-1]

    # If the trend is going the wrong way, we aren't heading towards a fault
    if (is_increasing_fault and slope <= 0.1) or (not is_increasing_fault and slope >= -0.1):
        return None, slope

    # Calculate intervals until threshold is breached
    intervals_to_breach = (threshold - current_val) / slope
    return max(0, intervals_to_breach), slope

data = ctrl.getAllData()
def analyze_and_predict(payload):
    alerts = []
    
    for device_id, data in payload.items():
        if "solar-inverter" not in device_id:
            continue

        # Extract current data
        inv_temp = data.get("temperature", 0)
        input_p = data.get("input_power", 0)
        total_p = data.get("total_power", 0)
        
        # Calculate derived metrics
        efficiency = (total_p / input_p) * 100 if input_p > 10000 else 100.0

        # Update historical buffers
        device_history[device_id]["temperature"].append(inv_temp)
        device_history[device_id]["efficiency"].append(efficiency)

        # ---------------------------------------------------------
        # PREDICTION 1: Thermal Runaway (Predicting Overheating)
        # Assuming critical shutdown happens at 65°C
        # ---------------------------------------------------------
        temp_history = device_history[device_id]["temperature"]
        ttf_intervals, temp_slope = predict_time_to_threshold(temp_history, threshold=65.0, is_increasing_fault=True)
        
        if ttf_intervals is not None and ttf_intervals < 30: # If it will hit 65°C in less than 30 intervals
            alerts.append({
                "device_id": device_id,
                "anomaly": "PREDICTIVE: Impending Thermal Shutdown",
                "details": f"Temp is {inv_temp}°C and climbing at {round(temp_slope, 2)}°C per tick. Projected to breach 65°C in {round(ttf_intervals)} ticks. Clean filters.",
                "severity": "CRITICAL"
            })

        # ---------------------------------------------------------
        # PREDICTION 2: Efficiency Degradation (Predicting Soiling/Fault)
        # Assuming efficiency dropping below 95% is a fault
        # ---------------------------------------------------------
        eff_history = device_history[device_id]["efficiency"]
        ttf_eff, eff_slope = predict_time_to_threshold(eff_history, threshold=95.0, is_increasing_fault=False)
        
        if ttf_eff is not None and ttf_eff < 60:
            alerts.append({
                "device_id": device_id,
                "anomaly": "PREDICTIVE: Efficiency Degradation",
                "details": f"Efficiency is dropping by {round(abs(eff_slope), 2)}% per tick. Will breach 95% threshold in {round(ttf_eff)} ticks.",
                "severity": "WARNING"
            })

    return alerts

if __name__ == "__main__":
    analyze_and_predict(data)