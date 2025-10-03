'''The refined code below uses the best elements of Code 2
(Context Manager, comprehensive scanning, robust verification)
but simplifies the structure, cleans up the imports, and slightly optimizes the inner loop
by reducing the number of times files are opened/closed.
It also makes the verify_part function cleaner and handles the device name dynamically.'''



import os
import sys
import json
import time
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from pymodbus.client import ModbusSerialClient
from pymodbus.client import ModbusBaseClient
from pymodbus import framer
from pymodbus.exceptions import ModbusException
from pymodbus.pdu import ExceptionResponse

BAUDRATES = [9600, 4800, 19200]
PARITIES = ['O', 'N', 'E']
SLAVES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
RESPONSE_TIMEOUT = 0.5
DELAY_BETWEEN_CHECKS = 0.05

@dataclass
class ModbusDevice:
    port: str
    baud: int
    parity: str
    slave_id: int
    device_name: str
    details: Dict[str, Any]

def load_config_files(cfg_path="auto_config_details.json", maps_path="modbus_registers.json") -> Optional[tuple[Dict, Dict]]:
    try:
        with open(cfg_path, 'r') as cfg_file:
            cfg = json.load(cfg_file)
        with open(maps_path, 'r') as modbus_file:
            maps = json.load(modbus_file)
        return cfg, maps
    except (FileNotFoundError, json.JSONDecodeError):
        return None

def find_parameter(device_name: str, parameter: str, maps: Dict) -> Optional[Dict]:
    device_info = maps.get(device_name)
    if not device_info:
        return None
    
    for block in device_info.values():
        param_data = block.get("data", {})
        if parameter in param_data:
            result = param_data[parameter].copy()
            result.update({
                "start_address": block.get("start_address"),
                "registers": block.get("registers")
            })
            return result
    return None

def read_modbus_register(client: ModbusSerialClient, slave_id: int, param_info: Dict, register_type: str) -> Optional[float]:
    try:
        address = param_info.get("start_address", 0) + param_info.get("offset", 0)
        num_registers = param_info.get("size", 1)
        
        if register_type == "input":
            regs = client.read_input_registers(address=address, count=num_registers, slave=slave_id)
        elif register_type == "holding":
            regs = client.read_holding_registers(address=address, count=num_registers, slave=slave_id)
        else:
            return None

        if regs.isError():
            if isinstance(regs, ExceptionResponse):
                 pass 
            return None 
        
        multiplier = param_info.get("m_f", 1)
        if multiplier == "NA":
            multiplier = 1.0

        data_format = param_info.get("format", "UINT16")
        if data_format.startswith('decode_') and data_format.endswith('_uint'):
            data_format = 'UINT16'
        
        value = client.convert_from_registers(regs.registers, getattr(client.DATATYPE, data_format))
        return value * multiplier

    except AttributeError:
        return None
    except ModbusException:
        return None
    except Exception:
        return None
    finally:
        time.sleep(DELAY_BETWEEN_CHECKS)

def get_device_config_details(client: ModbusSerialClient, device_name: str, slave_id: int, cfg: Dict, maps: Dict) -> Dict[str, Any]:
    device_details = {}
    try:
        config_list = cfg["devices"][device_name]["holding"]["config_list"]
    except KeyError:
        return {}

    for config_param in config_list:
        param_info = find_parameter(device_name, config_param, maps)
        if not param_info:
            continue
            
        register_type = "holding" if param_info["registers"] == "hr" else "input"

        value = read_modbus_register(client, slave_id, param_info, register_type)
        if value is not None:
            device_details[config_param] = value

    return device_details


def verify_part(client: ModbusSerialClient, device_name: str, slave_id: int, cfg: Dict, maps: Dict) -> bool:
    try:
        device_params = cfg["devices"][device_name]
        
        param_checks = [
            (device_params["input"]["param_list"], "input"),
            (device_params["holding"]["param_list"], "holding")
        ]
    except KeyError:
        return False

    for param_list, register_type in param_checks:
        for param in param_list:
            param_info = find_parameter(device_name, param, maps)
            if not param_info:
                return False 

            data_value = read_modbus_register(client, slave_id, param_info, register_type)
            
            if data_value is None:
                return False 

            min_val, max_val = cfg["param_range"].get(param, [float('-inf'), float('inf')])
            if not (min_val <= data_value <= max_val):
                return False 
    
    return True

def detect_comm_details(port: str, cfg: Dict, maps: Dict) -> List[ModbusDevice]:
    parts_list: List[ModbusDevice] = []
    device_names = list(cfg["devices"].keys())

    for baud in BAUDRATES:
        for par in PARITIES:
            client = ModbusSerialClient(
                port=port, 
                baudrate=baud, 
                parity=par, 
                framer=framer.FramerType.RTU, 
                timeout=RESPONSE_TIMEOUT
            )
            
            with client as conn:
                if not conn.is_socket_open():
                    continue 

                for slave_id in SLAVES:
                    for device_name in device_names:
                        
                        if verify_part(conn, device_name, slave_id, cfg, maps):
                            
                            device_details = get_device_config_details(conn, device_name, slave_id, cfg, maps)
                            
                            print(f"Found device '{device_name}' at SLAVE ID {slave_id} on {port} (Baud: {baud}, Parity: {par})")
                            parts_list.append(ModbusDevice(
                                port=port, 
                                baud=baud, 
                                parity=par, 
                                slave_id=slave_id, 
                                device_name=device_name,
                                details=device_details
                            ))
                        
                        time.sleep(0.1) 
                        
    return parts_list

def detect_all_devices(usb_ports: List[str]) -> List[ModbusDevice]:
    config_data = load_config_files()
    if config_data is None:
        print("Cannot proceed without configuration files.")
        return []

    cfg, maps = config_data
    all_devices_found: List[ModbusDevice] = []

    for port in usb_ports:
        print(f"\n--- Scanning port: {port} ---")
        found_on_port = detect_comm_details(port, cfg, maps)
        all_devices_found.extend(found_on_port)
    
    return all_devices_found

if __name__ == "__main__":
    usb_ports_to_scan = ["/dev/ttyUSB0", "/dev/ttyUSB1"] 
    
    print(f"--- Starting scan on specified ports: {usb_ports_to_scan} ---")
    all_found_devices = detect_all_devices(usb_ports_to_scan)
    
    if all_found_devices:
        print("\n--- All detected devices: ---")
        for dev in all_found_devices:
            print(dev)
    else:
        print("No devices were detected on the specified ports.")