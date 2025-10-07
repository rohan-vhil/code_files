import time
import requests
import logging
import control.control_base as ctrl
import re

class DeviceStatusReporter:
    def __init__(self, poll_interval=60):
        self.last_known_statuses = {}
        self.poll_interval = poll_interval
        self.api_url = "https://app.enercog.com/ui/client/no-auth/device-status"
        self.session = requests.Session()
        self.logger = logging.getLogger("DeviceStatusReporter")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def check_and_report(self):
        try:
            all_data = ctrl.getAllData()
        except Exception as e:
            self.logger.error(f"Could not get all device data: {e}")
            return

        if not all_data or 'timestamp' not in all_data:
            self.logger.info("No data available to check.")
            return

        timestamp = all_data.pop('timestamp')
        
        current_statuses = {}
        inverter_count = 1
        meter_count = 1
        
        sorted_device_ids = sorted(all_data.keys())

        for device_id in sorted_device_ids:
            device_data = all_data.get(device_id, {})
            device_status_info = {}
            
            total_power = device_data.get("total_power", 0)
            device_status_info["status"] = "online" if total_power and total_power > 0 else "offline"

            device_type = device_data.get("type", "")
            if "inverter" in device_type:
                device_key = f"inverter_{inverter_count}"
                inverter_count += 1
                
                mppt_status = {}
                mppt_data = device_data.get("mppt", {})
                if mppt_data:
                    mppt_power_keys = sorted([k for k in mppt_data if k.endswith("_power")])
                    for i, key in enumerate(mppt_power_keys, 1):
                        mppt_name = f"mppt_{i}"
                        mppt_power = mppt_data.get(key, 0)
                        mppt_status[mppt_name] = "online" if mppt_power and mppt_power > 0 else "offline"
                
                string_status = {}
                string_data = device_data.get("string", {})
                if string_data:
                    string_keys = sorted(string_data.keys(), key=lambda x: int(re.search(r'\d+', x).group()))
                    for i, key in enumerate(string_keys, 1):
                        string_name = f"string_{i}"
                        string_current = string_data.get(key, 0)
                        string_status[string_name] = "online" if string_current and string_current > 0 else "offline"
                
                if mppt_status:
                    device_status_info["mppt"] = mppt_status
                if string_status:
                    device_status_info["string"] = string_status
            else:
                device_key = f"meter_{meter_count}"
                meter_count += 1

            current_statuses[device_key] = device_status_info

        if self.last_known_statuses == current_statuses:
            return

        payload_content = {"timestamp": timestamp * 1000}
        payload_content.update(current_statuses)
        payload = [payload_content]
        
        self.logger.info(f"STATUS CHANGE DETECTED. Sending payload: {payload}")
        
        try:
            response = self.session.post(self.api_url, json=payload, timeout=10)
            response.raise_for_status()
            self.last_known_statuses = current_statuses
            self.logger.info(f"API update successful (Code: {response.status_code}).")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API update failed: {e}")

    def run(self):
        self.logger.info("Device Status Reporter thread started.")
        startup_delay = 20
        self.logger.info(f"Waiting for a {startup_delay}-second grace period before the first check.")
        time.sleep(startup_delay)

        while True:
            self.check_and_report()
            time.sleep(self.poll_interval)