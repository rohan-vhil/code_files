'''Device Status Reporter Code Understanding
'''


import time
import requests
import logging
import control.control_base as ctrl

class DeviceStatusReporter:
    def __init__(self, poll_interval=60):
        self.last_known_statuses = {}
        self.poll_interval = poll_interval
        self.api_url = "https://app.enercog.com/ui/client/no-auth/device-status"
        self.session = requests.Session()
        self.logger = logging.getLogger("DeviceStatusReporter")
        self.logger.setLevel(logging.INFO)
        
        self.logger.propagate = False 

        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def check_and_report(self):
        try:
            all_data = ctrl.getAllData()
        except Exception as e:
            self.logger.error(f"Could not get all data: {e}")
            return
        
        if not all_data:
            self.logger.info("No data available to check.")
            return

        current_statuses_payload = {}
        report_timestamp = all_data.get("timestamp", int(time.time())) * 1000

        for device_id, device_data in all_data.items():
            if device_id == "timestamp":
                continue
            
            if not isinstance(device_data, dict):
                self.logger.warning(f"Skipping invalid data for device_id: {device_id}")
                continue

            device_status_block = {}
            
            total_power = device_data.get("total_power")
            device_status_block["status"] = "online" if total_power and total_power > 0 else "offline"

            mppt_data = device_data.get("mppt")
            if mppt_data and isinstance(mppt_data, dict):
                mppt_status = {}
                for key, value in mppt_data.items():
                    if key.endswith("_current") and "mppt" in key:
                        mppt_name = key.replace("_current", "").replace("mppt", "mppt_")
                        mppt_status[mppt_name] = "online" if value and value > 0 else "offline"
                if mppt_status:
                    device_status_block["mppt"] = mppt_status

            string_data = device_data.get("string")
            if string_data and isinstance(string_data, dict):
                string_status = {}
                for key, value in string_data.items():
                    if key.endswith("_current") and "string" in key:
                        string_name = key.replace("_current", "").replace("string", "string_")
                        string_status[string_name] = "online" if value and value > 0 else "offline"
                if string_status:
                    device_status_block["string"] = string_status
            
            current_statuses_payload[device_id] = device_status_block

        if not current_statuses_payload:
            self.logger.info("No device data processed.")
            return

        if self.last_known_statuses == current_statuses_payload:
            self.logger.info("No status changes detected.")
            return

        self.logger.info("Status change detected. Sending new payload.")
        
        final_payload_dict = {"timestamp": report_timestamp}
        final_payload_dict.update(current_statuses_payload)
        payload = [final_payload_dict]
        
        self.logger.info(f"Sending payload: {payload}")
        
        try:
            response = self.session.post(self.api_url, json=payload, timeout=10)
            response.raise_for_status()
            self.last_known_statuses = current_statuses_payload
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