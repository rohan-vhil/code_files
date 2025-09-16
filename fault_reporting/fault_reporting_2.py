'''rewritten all by gemini itself
Here is the refactored code, structured into distinct classes
for configuration management, API communication
and device monitoring. This improved structure
enhances clarity and maintainability.'''




import sys
import json
import time
import logging
import requests
import uuid
import random
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian

class ApiClient:
    def __init__(self, api_endpoint):
        self.api_endpoint = api_endpoint

    def report_alert(self, payload):
        try:
            response = requests.post(self.api_endpoint, json=payload, timeout=10)
            response.raise_for_status()
            logging.info(f"API Success: Reported '{payload.get('alert_message')}' for {payload.get('device_id')} with status '{payload.get('status')}'")
        except requests.exceptions.RequestException as e:
            logging.error(f"API Failure: Could not report alert for {payload.get('device_id')}: {e}")

class Config:
    def __init__(self, cfg_path, maps_path, codes_path):
        try:
            with open(cfg_path) as f:
                self.installer_config = json.load(f)
            with open(maps_path) as f:
                self.modbus_maps = json.load(f)
            with open(codes_path) as f:
                self.error_codes = json.load(f)
        except IOError as e:
            logging.critical(f"Configuration Failure: Could not load file - {e}")
            sys.exit(1)
        
        self.api_endpoint = "https://example.com/api/v1/alerts"

    def get_device_configs(self):
        return self.installer_config.get("device_list", [])
    
    def get_modbus_map(self, part_num):
        if part_num in self.modbus_maps:
            return {'map': list(self.modbus_maps[part_num].values())}
        return None

    def get_error_map(self, part_num):
        return self.error_codes.get(part_num, {})

class FaultProcessor:
    def __init__(self, modbus_map, error_map):
        self.modbus_map = modbus_map
        self.error_map = error_map
        self.active_bitfield_alerts = {}
        self.active_code_alert = None
        self.fault_code = 0
        self.batch_fault_code_index = -1
        self.offset_fault_code = 0
        self.fault_size = 0
        self.error_init = self._initialize_processor()

    def _initialize_processor(self):
        is_initialized = False
        for i, batch_config in enumerate(self.modbus_map.get('map', [])):
            if 'fault' in batch_config.get("data", {}):
                self.batch_fault_code_index = i
                self.offset_fault_code = batch_config["data"]['fault']["offset"]
                self.fault_size = batch_config["data"]["fault"]["size"]
                is_initialized = True
        return is_initialized

    def decode(self, modbus_data):
        if not self.error_init:
            return []
        
        fault_config = self.error_map.get('Fault', {})
        if not fault_config:
            return []

        try:
            fault_registers = modbus_data[self.batch_fault_code_index][self.offset_fault_code : self.offset_fault_code + self.fault_size]
            
            if self.fault_size > 1:
                self.fault_code = BinaryPayloadDecoder.fromRegisters(fault_registers, Endian.BIG).decode_32bit_uint()
            else:
                self.fault_code = fault_registers[0]
            
            if fault_config.get('type') == 'bitfield':
                return self._decode_bitfield_faults(fault_config)
            else:
                return self._decode_code_faults(fault_config)

        except (IndexError, KeyError, TypeError) as e:
            logging.warning(f"Fault decoding failed: {e}")
            return []

    def _decode_bitfield_faults(self, fault_config):
        alerts_to_report = []
        all_possible_alerts = set(fault_config.get('codes', {}).values())
        active_now = set()

        for bit, message in fault_config.get('codes', {}).items():
            if self.fault_code & (1 << int(bit)):
                active_now.add(message)
                if message not in self.active_bitfield_alerts:
                    ref = str(uuid.uuid4())
                    self.active_bitfield_alerts[message] = ref
                    alerts_to_report.append(self._create_payload('new', message, ref))
        
        resolved_alerts = set(self.active_bitfield_alerts.keys()) - active_now
        for message in resolved_alerts:
            ref = self.active_bitfield_alerts.pop(message)
            alerts_to_report.append(self._create_payload('resolved', message, ref))
        
        return alerts_to_report

    def _decode_code_faults(self, fault_config):
        alerts_to_report = []
        codes = fault_config.get('codes', fault_config)
        current_message = codes.get(str(self.fault_code))

        if current_message and current_message == self.active_code_alert:
            return []

        if self.active_code_alert:
            ref = str(uuid.uuid4())
            alerts_to_report.append(self._create_payload('resolved', self.active_code_alert, ref))
            self.active_code_alert = None
        
        if current_message and current_message.lower() != 'noerror':
            self.active_code_alert = current_message
            ref = str(uuid.uuid4())
            alerts_to_report.append(self._create_payload('new', self.active_code_alert, ref))
            
        return alerts_to_report

    def _create_payload(self, status, message, ref):
        payload = {
            'timestamp': time.time(),
            'status': status,
            'alert_message': message,
            'alert_ref': ref
        }
        if status == 'new':
            payload.update({
                'severity': 'medium',
                'level': 'device',
                'alert_code': str(self.fault_code)
            })
        return payload

class Device:
    def __init__(self, device_config, config_loader, api_client):
        self.part_num = device_config.get("part_num")
        self.id = device_config.get("device_id") or device_config.get("device ID")
        self.api_client = api_client
        
        modbus_map = config_loader.get_modbus_map(self.part_num)
        error_map = config_loader.get_error_map(self.part_num)
        
        self.processor = None
        if modbus_map and error_map:
            self.processor = FaultProcessor(modbus_map, error_map)

    def is_monitorable(self):
        return self.processor and self.processor.error_init

    def check_faults(self, modbus_data):
        if not self.is_monitorable():
            return
        
        alerts = self.processor.decode(modbus_data)
        for alert_payload in alerts:
            alert_payload['device_id'] = self.id
            self.api_client.report_alert(alert_payload)

class Application:
    def __init__(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.config = Config('installer_cfg.json', 'mappings.json', 'error_codes.json')
        self.api_client = ApiClient(self.config.api_endpoint)
        self.devices = self._initialize_devices()

    def _initialize_devices(self):
        initialized_devices = []
        for device_cfg in self.config.get_device_configs():
            device = Device(device_cfg, self.config, self.api_client)
            if device.id and device.is_monitorable():
                initialized_devices.append(device)
                logging.info(f"Successfully initialized monitor for device: {device.id} ({device.part_num})")
            elif device.id:
                logging.warning(f"Could not initialize monitor for device: {device.id}. Check configuration.")
        return initialized_devices

    def _generate_simulated_data(self, device):
        processor = device.processor
        modbus_map = processor.modbus_map['map']
        
        simulated_data = []
        for block in modbus_map:
            simulated_data.append([0] * block['Length'])

        fault_config = processor.error_map.get('Fault', {})
        codes = fault_config.get('codes', fault_config)
        
        if not codes or "0" in codes and len(codes) <= 1:
             return simulated_data

        fault_code_to_simulate = 0
        if fault_config.get('type') == 'bitfield':
            random_bit = int(random.choice(list(codes.keys())))
            fault_code_to_simulate = 1 << random_bit
        else:
            valid_codes = [k for k,v in codes.items() if v.lower() != 'noerror']
            if valid_codes:
                fault_code_to_simulate = int(random.choice(valid_codes))

        if fault_code_to_simulate > 0:
            simulated_data[processor.batch_fault_code_index][processor.offset_fault_code] = fault_code_to_simulate
            logging.info(f"Checking faults for {device.id} with simulated code {fault_code_to_simulate}...")
        
        return simulated_data

    def run(self):
        if not self.devices:
            logging.error("No monitorable devices found. Exiting.")
            return

        while True:
            try:
                for device in self.devices:
                    simulated_data = self._generate_simulated_data(device)
                    device.check_faults(simulated_data)
                
                time.sleep(10)

            except KeyboardInterrupt:
                logging.info("Shutting down application.")
                break
            except Exception as e:
                logging.error(f"An unexpected error occurred in the main loop: {e}")
                time.sleep(30)

if __name__ == "__main__":
    app = Application()
    app.run()