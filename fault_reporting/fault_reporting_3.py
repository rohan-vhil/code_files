'''created client and read fault register

This version incorporates pymodbus
for live data acquisition from RTU devices
as specified in your configuration. It dynamically creates a Modbus client
for each device, reads the necessary register blocks to find the fault'''


import sys
import json
import time
import logging
import requests
import uuid

from pymodbus.client import ModbusSerialClient
from pymodbus.exceptions import ModbusException
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

class ModbusReader:
    def __init__(self, rtu_details):
        self.client = ModbusSerialClient(
            port=rtu_details.get("port"),
            baudrate=int(rtu_details.get("baudrate", 9600)),
            parity=rtu_details.get("parity", "N"),
            stopbits=int(rtu_details.get("stop_bits", 1)),
            timeout=3
        )

    def read_registers(self, start_address, count, slave_id, register_type):
        if not self.client.connect():
            logging.error(f"Modbus connection failed to port {self.client.port}")
            return None
        
        try:
            read_function_map = {
                'ir': self.client.read_input_registers,
                'hr': self.client.read_holding_registers
            }
            
            read_function = read_function_map.get(register_type)
            if not read_function:
                logging.error(f"Unsupported register type: {register_type}")
                return None

            response = read_function(address=start_address, count=count, slave=slave_id)
            
            if response.isError():
                logging.error(f"Modbus Error on slave {slave_id}: {response}")
                return None
            
            return response.registers

        except ModbusException as e:
            logging.error(f"Modbus exception on slave {slave_id}: {e}")
            return None
        finally:
            self.client.close()

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
                self.fault_code = BinaryPayloadDecoder.fromRegisters(fault_registers, byteorder=Endian.BIG, wordorder=Endian.BIG).decode_32bit_uint()
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
        payload = {'timestamp': time.time(), 'status': status, 'alert_message': message, 'alert_ref': ref}
        if status == 'new':
            payload.update({'severity': 'medium', 'level': 'device', 'alert_code': str(self.fault_code)})
        return payload

class Device:
    def __init__(self, device_config, config_loader, api_client):
        self.part_num = device_config.get("part_num")
        self.id = device_config.get("device_id") or device_config.get("device ID")
        self.api_client = api_client
        self.modbus_map = config_loader.get_modbus_map(self.part_num)
        self.rtu_details = device_config.get("modbus-rtu_details", {})
        self.slave_id = int(self.rtu_details.get("slave_id", 1))
        
        self.reader = ModbusReader(self.rtu_details) if self.rtu_details else None
        error_map = config_loader.get_error_map(self.part_num)
        
        self.processor = None
        if self.modbus_map and error_map:
            self.processor = FaultProcessor(self.modbus_map, error_map)

    def is_monitorable(self):
        return self.reader and self.processor and self.processor.error_init

    def poll_and_process(self):
        if not self.is_monitorable():
            return

        all_register_data = []
        is_read_successful = True
        
        for block_config in self.modbus_map.get('map', []):
            start_address = block_config['start_address']
            count = block_config['Length']
            register_type = block_config.get('registers', 'hr')
            
            registers = self.reader.read_registers(start_address, count, self.slave_id, register_type)
            
            if registers is None:
                logging.error(f"Failed to read block at {start_address} for device {self.id}")
                is_read_successful = False
                break
            all_register_data.append(registers)

        if is_read_successful:
            alerts = self.processor.decode(all_register_data)
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
            if device_cfg.get("comm_type") != "modbus-rtu":
                continue
            device = Device(device_cfg, self.config, self.api_client)
            if device.id and device.is_monitorable():
                initialized_devices.append(device)
                logging.info(f"Successfully initialized monitor for device: {device.id} ({device.part_num})")
            elif device.id:
                logging.warning(f"Could not initialize monitor for device: {device.id}. Check configuration.")
        return initialized_devices

    def run(self):
        if not self.devices:
            logging.error("No monitorable Modbus RTU devices found. Exiting.")
            return

        while True:
            try:
                for device in self.devices:
                    logging.info(f"Polling device: {device.id}")
                    device.poll_and_process()
                
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
