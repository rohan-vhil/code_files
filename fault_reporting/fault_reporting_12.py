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
        
        print("--- FaultProcessor: Initializing... ---")
        try:
            with open(error_codes_path) as error_json:
                self.error_map = json.load(error_json)
            print(f"--- FaultProcessor: Error codes loaded successfully from {error_codes_path} ---")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"--- FaultProcessor: CRITICAL - Failed to load error codes file: {e} ---")
            logging.critical(f"Failed to load error codes file: {e}")
            self.error_map = {}

    def _send_faults_to_api(self, payload: list):
        if not payload:
            return
        
        print(f"--- FaultProcessor: Preparing to send payload to API...")
        print(f"--- FaultProcessor: PAYLOAD TO BE SENT:\n{json.dumps(payload, indent=4)} ---")
        try:
            response = requests.post(self.api_url, json=payload, timeout=10)
            response.raise_for_status()
            # To count devices, we look inside the first object and subtract 1 for the timestamp key
            fault_object = payload[0]
            device_count = len(fault_object) - 1
            print(f"--- FaultProcessor: API call successful for {device_count} device(s). ---")
            logging.info(f"Successfully sent new fault data for {device_count} device(s) to API.")
        except requests.exceptions.RequestException as e:
            print(f"--- FaultProcessor: API call FAILED. Error: {e} ---")
            logging.error(f"Failed to send faults to API: {e}")

    def stop(self):
        self._running = False
        print("--- FaultProcessor: Stop signal received. ---")
        logging.info("Fault processor stop signal received.")

    def run(self):
        self._running = True
        print("--- FaultProcessor: Thread started. Beginning main loop. ---")
        
        if not self.error_map:
            print("--- FaultProcessor: CRITICAL - Cannot run, error map is not loaded. ---")
            logging.critical("Fault processor cannot run, error map is not loaded.")
            return

        while self._running:
            print(f"\n--- FaultProcessor: Starting new poll cycle at {time.ctime()} ---")
            try:
                fault_data = ctrl.getFaultData()
                print(f"--- FaultProcessor: Received data from getFaultData(): {fault_data} ---")
                print(f"--- FaultProcessor: Current state of last_faults before check: {self.last_faults} ---")
                
                new_faults_by_device = {}

                for device_id, fault_dict in fault_data.items():
                    fault_code = fault_dict.get('fault', 0)
                    part_num = device_id.split(':')[1]

                    print(f"\n--- FaultProcessor: > Checking device '{device_id}' (Part: {part_num}) | Received Code: {fault_code}")
                    last_code = self.last_faults.get(device_id)

                    if fault_code == last_code:
                        print(f"--- FaultProcessor: > No change for '{device_id}'. Code ({fault_code}) is same as last known code ({last_code}). No action needed.")
                        continue

                    self.last_faults[device_id] = fault_code

                    if fault_code != 0:
                        print(f"--- FaultProcessor: >>>> NEW/CHANGED FAULT DETECTED for '{device_id}'. Code changed from {last_code} to {fault_code}. <<<<")
                        
                        device_config = self.error_map.get(part_num, {}).get("Fault", self.error_map.get(part_num, {}).get("fault", {}))
                        
                        if not device_config:
                            print(f"--- FaultProcessor: > Warning: No 'Fault' definition found for part_num '{part_num}' in error_codes.json")
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
                    else:
                        print(f"--- FaultProcessor: >>>> FAULT CLEARED for '{device_id}'. Code changed from {last_code} to 0. <<<<")


                if new_faults_by_device:
                    print("\n--- FaultProcessor: New faults found! Preparing to send to API. ---")
                    
                    # **REVISED PAYLOAD FORMATTING**
                    # Create a single object with the timestamp and all new device faults
                    payload_object = {
                        "timestamp": int(time.time()),
                        **new_faults_by_device
                    }
                    
                    # Wrap the final object in a list before sending
                    self._send_faults_to_api([payload_object])
                else:
                    print("\n--- FaultProcessor: No new faults to report in this cycle. ---")

            except Exception as e:
                print(f"--- FaultProcessor: An unexpected error occurred in the fault processing loop: {e} ---")
                logging.error(f"An unexpected error occurred in the fault processing loop: {e}")

            print(f"--- FaultProcessor: Poll cycle finished. Sleeping for {self.poll_interval} seconds. ---")
            time.sleep(self.poll_interval)
        
        print("--- FaultProcessor: Loop has been stopped. ---")
        logging.info("Fault processor has stopped.")