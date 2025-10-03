'''The current approach is O(P⋅B⋅Y⋅S⋅D⋅R), where:

P: number of Ports (3)

B: number of Baud rates (3)

Y: number of Parity types (3)

S: number of Slave IDs (12)

D: number of Device types (e.g., 2)

R: number of Register reads per verification (e.g., 5-10)

The total number of Modbus transaction attempts is around 3⋅3⋅3⋅12⋅2⋅1≈648 per port, or ≈1944 total (excluding the R reads). Since each read takes time (e.g., 0.5s timeout +0.05s delay), the process is slow.

The primary way to reduce time complexity is to eliminate the unnecessary device-type check (D) in the innermost loop by attempting a universal or known register read first.

Optimized Approach: Two-Phase Check
The optimized code uses a two-phase check:

Phase 1 (Basic Communication Test): Instead of immediately running the full verify_part for every device type, the script attempts a single, basic register read (e.g., address 0, count 1, Input Registers) for the current Port/Baud/Parity/Slave_ID.

Result: If this simple read succeeds, it proves something is talking Modbus RTU at that specific address and communication setting. This collapses the D factor for the vast majority of failures.

Phase 2 (Device Identification): If Phase 1 succeeds, only then does the script iterate through all Device Names (D) and run the full verify_part check to determine the exact device type.

This changes the complexity to approximately O(P⋅B⋅Y⋅S⋅(1+D⋅R 
successful
​
 )). The 1 represents the single quick Phase 1 read. This is a massive time reduction because most combinations will fail Phase 1 quickly, eliminating D⋅R reads entirely.

The detect_comm_details function is updated with this logic.'''



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
            print(f"  > Read Config: {config_param} = {value}")

    return device_details


def verify_part(client: ModbusSerialClient, device_name: str, slave_id: int, cfg: Dict, maps: Dict) -> bool:
    is_verified = True
    try:
        device_params = cfg["devices"][device_name]
        
        param_checks = [
            (device_params["input"]["param_list"], "input"),
            (device_params["holding"]["param_list"], "holding")
        ]
    except KeyError:
        return False

    print(f"  * Verifying device '{device_name}' on slave ID {slave_id}:")
    for param_list, register_type in param_checks:
        for param in param_list:
            param_info = find_parameter(device_name, param, maps)
            if not param_info:
                is_verified = False
                break 

            data_value = read_modbus_register(client, slave_id, param_info, register_type)
            
            if data_value is None:
                print(f"    - FAIL: Could not read register for parameter '{param}'")
                is_verified = False
                break 

            min_val, max_val = cfg["param_range"].get(param, [float('-inf'), float('inf')])
            if not (min_val <= data_value <= max_val):
                print(f"    - FAIL: Read value {data_value} for '{param}' is out of range ({min_val} to {max_val})")
                is_verified = False
                break 
            
            print(f"    - SUCCESS: '{param}' = {data_value}")

        if not is_verified:
            break
    
    return is_verified

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
                    
                    print(f"\nScanning: Port={port}, Baud={baud}, Parity={par}, Slave ID={slave_id}")
                    
                    # --- OPTIMIZATION PHASE 1: Basic Communication Check ---
                    try:
                        # Attempt a quick, standard read (e.g., Input Register 0 or 1, count 1)
                        # Success proves a device is listening at this address/comm setting.
                        test_regs = conn.read_input_registers(address=0, count=1, slave=slave_id)
                        
                        if test_regs.isError():
                            print(f"  - Basic Test: Modbus error or no response.")
                            continue
                        
                        print(f"  - Basic Test: SUCCESS (Response received). Starting full device verification.")
                        time.sleep(DELAY_BETWEEN_CHECKS)
                    
                    except ModbusException:
                        print(f"  - Basic Test: Communication error.")
                        continue
                    except Exception:
                        print(f"  - Basic Test: General exception during test read.")
                        continue
                    
                    # --- OPTIMIZATION PHASE 2: Device Identification (Only runs if Phase 1 succeeded) ---
                    for device_name in device_names:
                        
                        if verify_part(conn, device_name, slave_id, cfg, maps):
                            
                            print(f"\n🎉 FOUND DEVICE: '{device_name}'")
                            print(f"  > Comm Details: Port={port}, Baud={baud}, Parity={par}, Slave ID={slave_id}")
                            print("  > Reading Configuration Details...")
                            device_details = get_device_config_details(conn, device_name, slave_id, cfg, maps)
                            
                            parts_list.append(ModbusDevice(
                                port=port, 
                                baud=baud, 
                                parity=par, 
                                slave_id=slave_id, 
                                device_name=device_name,
                                details=device_details
                            ))
                            
                        else:
                            pass
                        
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
    usb_ports_to_scan = ["/dev/ttyV0", "/dev/ttyV1", "/dev/ttyUSB0"] 
    
    print(f"--- Starting scan on specified ports: {usb_ports_to_scan} ---")
    all_found_devices = detect_all_devices(usb_ports_to_scan)
    
    if all_found_devices:
        print("\n--- All detected devices: ---")
        for dev in all_found_devices:
            print(dev)
    else:
        print("\nNo devices were detected on the specified ports.")