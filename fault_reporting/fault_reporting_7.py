import json
import time
import logging
import requests
from control import control_base as ctrl

class FaultProcessor:
    def __init__(self, error_codes_path='error_codes.json', poll_interval=5):
        self.api_url = "https://example.com/docs/save_fault"
        self.last_faults = {}
        self.poll_interval = poll_interval
        self._running = False
        try:
            with open(error_codes_path) as error_json:
                self.error_map = json.load(error_json)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logging.critical(f"Failed to load error codes file: {e}")
            self.error_map = {}

    def _send_faults_to_api(self, payload: dict):
        if not payload:
            return
        try:
            response = requests.post(self.api_url, json=payload, timeout=10)
            response.raise_for_status()
            device_count = len(payload) - 1
            logging.info(f"Successfully sent new fault data for {device_count} device(s) to API.")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to send faults to API: {e}")

    def stop(self):
        self._running = False
        logging.info("Fault processor stop signal received.")

    def run(self):
        self._running = True
        logging.info("Fault processor started.")
        
        if not self.error_map:
            logging.critical("Fault processor cannot run, error map is not loaded.")
            return

        while self._running:
            try:
                # This function is called from your control_base module
                fault_data = ctrl.getFaultData()
                new_faults_by_device = {}

                for device_id, fault_code in fault_data.items():
                    last_code = self.last_faults.get(device_id)

                    if fault_code != 0 and fault_code != last_code:
                        self.last_faults[device_id] = fault_code
                        
                        device_config = self.error_map.get(device_id, {}).get("Fault", {})
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

                        elif fault_type == 'code':
                            code_key = str(fault_code)
                            if code_key in fault_definitions:
                                fault_info = fault_definitions[code_key]
                                new_faults_by_device[device_id] = {
                                    "fault_code": code_key,
                                    "fault_message": fault_info.get("fault_message"),
                                    "severity": fault_info.get("severity")
                                }

                    elif fault_code == 0 and last_code != 0:
                        self.last_faults[device_id] = 0
                
                if new_faults_by_device:
                    final_payload = {"timestamp": time.time(), **new_faults_by_device}
                    self._send_faults_to_api(final_payload)

            except Exception as e:
                logging.error(f"An unexpected error occurred in the fault processing loop: {e}")

            time.sleep(self.poll_interval)
        
        logging.info("Fault processor has stopped.")