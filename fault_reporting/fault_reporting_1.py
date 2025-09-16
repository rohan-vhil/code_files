'''This version is specifically tailored to parse
the structures of your mappings.json, installer_cfg.json, and error_codes.json
files to monitor device faults. It correctly identifies the 'polycab' device,
locates its fault register, and decodes the fault values as a bitfield.

updating and changing existing firmware code'''




import sys
import json
import enum
import time
import logging
import requests
import uuid

from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian

class ErrorRegister:
    def __init__(self, addr_map, part_num, device_id, api_endpoint):
        self.batch_fault_code_index = -1
        self.offset_fault_code = 0
        self.fault_size = 0
        self.batch_device_state_index = -1
        self.offset_device_state = 0
        self.error_map = {}
        self.fault_code = 0
        self.status_code = 0
        self.device_state = ""
        self.active_alerts = {}
        self.error_init = False
        self.device_id = device_id
        self.api_endpoint = api_endpoint

        try:
            with open('error_codes.json') as error_json:
                self.error_map = json.load(error_json).get(part_num, {})
        except IOError as e:
            logging.error(f"Failed to load error_codes.json: {e}")
            return
        
        if not self.error_map:
            logging.warning(f"No error code definitions found for part number: {part_num}")
            return

        for i, batch_config in enumerate(addr_map['map']):
            if 'fault' in batch_config.get("data", {}):
                self.error_init = True
                self.batch_fault_code_index = i
                self.offset_fault_code = batch_config["data"]['fault']["offset"]
                self.fault_size = batch_config["data"]["fault"]["size"]
            
            if 'device_state' in batch_config.get("data", {}):
                self.error_init = True
                self.batch_device_state_index = i
                self.offset_device_state = batch_config["data"]['device_state']["offset"]

    def update_alert_status(self, alert_message, is_active):
        if is_active and alert_message not in self.active_alerts:
            alert_ref = str(uuid.uuid4())
            self.active_alerts[alert_message] = alert_ref
            payload = {
                'timestamp': time.time(),
                'device_id': self.device_id,
                'status': 'new',
                'severity': 'medium',
                'level': 'device',
                'alert_message': alert_message,
                'alert_ref': alert_ref,
                'alert_code': str(self.fault_code)
            }
            self.report_fault_to_api(payload)

        elif not is_active and alert_message in self.active_alerts:
            alert_ref = self.active_alerts.pop(alert_message)
            payload = {
                'timestamp': time.time(),
                'device_id': self.device_id,
                'status': 'resolved',
                'alert_ref': alert_ref,
                'alert_message': alert_message
            }
            self.report_fault_to_api(payload)

    def decode_errors(self, data):
        if not self.error_init:
            return

        try:
            if self.batch_fault_code_index != -1 and self.fault_size > 0:
                fault_config = self.error_map.get('Fault', {})
                fault_registers = data[self.batch_fault_code_index][self.offset_fault_code : self.offset_fault_code + self.fault_size]
                
                if self.fault_size > 1:
                     self.fault_code = BinaryPayloadDecoder.fromRegisters(fault_registers, Endian.BIG).decode_32bit_uint()
                else:
                     self.fault_code = fault_registers[0]

                if fault_config.get('type') == 'bitfield':
                    for bit, message in fault_config.get('codes', {}).items():
                        is_fault_active = bool(self.fault_code & (1 << int(bit)))
                        self.update_alert_status(message, is_fault_active)
                elif str(self.fault_code) in fault_config:
                    # Logic for non-bitfield fault codes
                    fault_message = fault_config[str(self.fault_code)]
                    if fault_message != "Noerror":
                        self.update_alert_status(fault_message, True)
                    # Note: Add logic to clear non-bitfield errors if needed

        except (IndexError, KeyError, TypeError) as e:
            logging.warning(f"Error during fault decoding for device {self.device_id}: {e}")

    def report_fault_to_api(self, payload):
        try:
            response = requests.post(self.api_endpoint, json=payload, timeout=10)
            response.raise_for_status()
            logging.info(f"Successfully reported alert to API: {payload.get('alert_message')}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to report alert to API for {self.device_id}: {e}")

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    API_ENDPOINT = "https://example.com/api/v1/alerts"

    try:
        with open('installer_cfg.json') as f:
            installer_config = json.load(f)
        with open('mappings.json') as f:
            modbus_maps = json.load(f)
    except IOError as e:
        logging.critical(f"Failed to load configuration files: {e}")
        sys.exit(1)

    device_monitors = []
    for device in installer_config.get("device_list", []):
        part_num = device.get("part_num")
        device_id = device.get("device_id") or device.get("device ID")
        
        if not device_id:
            logging.warning(f"Skipping device with missing 'device_id': {device}")
            continue

        if part_num in modbus_maps:
            addr_map = {'map': list(modbus_maps[part_num].values())}
            monitor = ErrorRegister(addr_map, part_num, device_id, API_ENDPOINT)
            
            if monitor.error_init:
                device_monitors.append(monitor)
                logging.info(f"Initialized fault monitor for device {device_id} ({part_num})")
            else:
                logging.warning(f"Could not initialize fault monitor for {device_id}. Check mappings.json for 'fault' or 'device_state'.")

    while True:
        try:
            for monitor in device_monitors:
                if monitor.device_id.startswith("solar-inverter:polycab"):
                    
                    # Simulate data for Polycab Inverter
                    block1_data = [0] * 100
                    block2_data = [0] * 8

                    # Simulate DC_OVER_VOLT (bit 1) and OVER_TEMP (bit 7)
                    # Fault code = (2^1) + (2^7) = 2 + 128 = 130
                    fault_value = 130
                    block1_data[98] = fault_value
                    
                    simulated_modbus_data = [block1_data, block2_data]

                    logging.info(f"Checking for faults on device {monitor.device_id} with simulated fault code {fault_value}...")
                    monitor.decode_errors(simulated_modbus_data)
            
            time.sleep(10)

        except KeyboardInterrupt:
            logging.info("Shutting down fault monitor.")
            break
        except Exception as e:
            logging.error(f"An unexpected error occurred in the main loop: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()