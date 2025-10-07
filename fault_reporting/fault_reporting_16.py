'''
I understand the requirement. The payload for each device should always be a list, even if it only contains a single fault.

I have updated the fault_reporting.py script to ensure this consistency. The logic for code and hexadecimal types, as well as the "Fault Cleared" message, will now wrap their single fault object in a list.'''


import json
import time
import logging
import requests
from control import control_base as ctrl

class FaultProcessor:
    def __init__(self, error_codes_path='error_codes.json', poll_interval=5):
        self.api_url = "https://app.enercog.com/ui/client/no-auth/timescaledb/save-alerts"
        self.last_faults = {}
        self.poll_interval = poll_interval
        self._running = False
        try:
            with open(error_codes_path) as error_json:
                self.error_map = json.load(error_json)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logging.critical(f"Failed to load error codes file: {e}")
            self.error_map = {}

    def _send_faults_to_api(self, payload: list):
        if not payload:
            return
        
        print(f"--- FaultProcessor: PAYLOAD TO BE SENT:\n{json.dumps(payload, indent=4)} ---")
        try:
            response = requests.post(self.api_url, json=payload, timeout=10)
            response.raise_for_status()
            fault_object = payload[0]
            device_count = len(fault_object) - 1
            logging.info(f"Successfully sent new fault data for {device_count} device(s) to API.")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to send faults to API: {e}")

    def stop(self):
        self._running = False
        logging.info("Fault processor stop signal received.")

    def run(self):
        self._running = True
        
        if not self.error_map:
            logging.critical("Fault processor cannot run, error map is not loaded.")
            return

        while self._running:
            try:
                fault_data = ctrl.getFaultData()
                print(f"--- FaultProcessor: Received data from getFaultData(): {fault_data} ---")
                
                new_faults_by_device = {}

                for device_id, fault_dict in fault_data.items():
                    fault_code = fault_dict.get('fault', 0)
                    part_num = device_id.split(':')[1]
                    last_code = self.last_faults.get(device_id)

                    if fault_code == last_code:
                        continue

                    self.last_faults[device_id] = fault_code

                    if fault_code != 0:
                        device_config = self.error_map.get(part_num, {}).get("Fault", self.error_map.get(part_num, {}).get("fault", {}))
                        
                        if not device_config:
                            continue

                        fault_type = device_config.get("type")
                        fault_definitions = device_config.get("codes", {})
                        
                        if fault_type == 'bitfield':
                            active_faults = []
                            for bit_pos_str, fault_info in fault_definitions.items():
                                if fault_code & (1 << int(bit_pos_str)):
                                    active_faults.append({
                                        "fault_code": bit_pos_str,
                                        "fault_message": fault_info.get("fault_message"),
                                        "severity": fault_info.get("severity")
                                    })
                            if active_faults:
                                new_faults_by_device[device_id] = active_faults
                        
                        elif fault_type == 'hexadecimal':
                            hex_key_with_prefix = hex(fault_code).lower()
                            hex_key_without_prefix = hex_key_with_prefix[2:]
                            
                            fault_info = fault_definitions.get(hex_key_with_prefix) or fault_definitions.get(hex_key_without_prefix)
                            
                            if fault_info:
                                fault_obj = {
                                    "fault_code": hex_key_with_prefix,
                                    "fault_message": fault_info.get("fault_message"),
                                    "severity": fault_info.get("severity")
                                }
                                new_faults_by_device[device_id] = [fault_obj]

                        elif fault_type == 'code':
                            code_key = str(fault_code)
                            if code_key in fault_definitions:
                                fault_info = fault_definitions[code_key]
                                fault_obj = {
                                    "fault_code": code_key,
                                    "fault_message": fault_info.get("fault_message"),
                                    "severity": fault_info.get("severity")
                                }
                                new_faults_by_device[device_id] = [fault_obj]
                    elif last_code is not None:
                        fault_obj = {
                            "fault_code": "0",
                            "fault_message": "Fault Cleared",
                            "severity": "Info"
                        }
                        new_faults_by_device[device_id] = [fault_obj]

                if new_faults_by_device:
                    payload_object = {
                        "timestamp": int(time.time()),
                        **new_faults_by_device
                    }
                    self._send_faults_to_api([payload_object])

            except Exception as e:
                logging.error(f"An unexpected error occurred in the fault processing loop: {e}")

            time.sleep(self.poll_interval)
        
        logging.info("Fault processor has stopped.")




'''
--- FaultProcessor: Received data from getFaultData(): {'solar-inverter:MAC_100KTL:456': {'fault': 203}} ---
--- FaultProcessor: PAYLOAD TO BE SENT:
[
    {
        "timestamp": 1759329215,
        "solar-inverter:MAC_100KTL:456": [
            {
                "fault_code": "203",
                "fault_message": "PV Isolation Low",
                "severity": "High"
            }
        ]
    }
] ---
'''