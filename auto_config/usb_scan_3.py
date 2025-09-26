import os
import sys
from pymodbus.client import ModbusTcpClient, ModbusSerialClient
from pymodbus.client import ModbusBaseClient
from pymodbus import framer
from pymodbus.exceptions import ModbusException
from dataclasses import dataclass
import json
import time

sys.path.insert(0, "../")

SERIAL_PATH = "/dev/serial/by-id/"

BAUDRATES = [9600, 4800, 19200]
PARITIES = ['O', 'N', 'E']
SLAVES = [1, 2, 3, 4, 5, 6, 7]

@dataclass
class ModbusDevice:
    port: str
    baud: int
    parity: str
    slave_id: int
    device_name: str

def find_parameter(device_name, parameter, json_path="modbus_registers.json"):
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        return None

    device_info = data.get(device_name)
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

def verify_part(client, device_name, slave_id, cfg, maps):
    device_registers = maps.get(device_name)
    if not device_registers:
        return False
        
    try:
        device_params = cfg["devices"][device_name]
        input_params = device_params["input"]["param_list"]
        holding_params = device_params["holding"]["param_list"]
    except KeyError:
        return False

    for param_list, register_type in [(input_params, "input"), (holding_params, "holding")]:
        for param in param_list:
            param_info = find_parameter(device_name, param, json_path="modbus_registers.json")
            if not param_info:
                return False

            try:
                address = param_info.get("start_address", 0) + param_info.get("offset", 0)
                num_registers = param_info.get("size", 1)

                if register_type == "input":
                    regs = client.read_input_registers(address=address, count=num_registers, slave=slave_id)
                else:
                    regs = client.read_holding_registers(address=address, count=num_registers, slave=slave_id)

                if regs.isError():
                    return False
                
                multiplier = param_info.get("m_f", 1)
                if multiplier == "NA":
                    multiplier = 1

                try:
                    data_format = param_info.get("format", "UINT16")
                    if data_format.startswith('decode_') and data_format.endswith('_uint'):
                        data_format = 'UINT16'
                    
                    data_value = client.convert_from_registers(regs.registers, getattr(client.DATATYPE, data_format)) * multiplier
                except AttributeError:
                    return False
                
                min_val, max_val = cfg["param_range"].get(param, [float('-inf'), float('inf')])
                if not (min_val <= data_value <= max_val):
                    return False
                
            except ModbusException:
                return False
            except Exception:
                return False
    
    return True

def detect_comm_details(port):
    parts_list = []
    
    try:
        with open("auto_config_details.json", 'r') as cfg_file:
            cfg = json.load(cfg_file)
        with open("modbus_registers.json", 'r') as modbus_file:
            maps = json.load(modbus_file)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

    for baud in BAUDRATES:
        for par in PARITIES:
            try:
                client = ModbusSerialClient(port=port, baudrate=baud, parity=par, framer=framer.FramerType.RTU, timeout=0.5)
                with client as conn:
                    for slave_id in SLAVES:
                        for device_name in cfg["devices"]:
                            if verify_part(conn, device_name, slave_id, cfg, maps):
                                parts_list.append(ModbusDevice(port=port, baud=baud, parity=par, slave_id=slave_id, device_name=device_name))
                            
                            time.sleep(0.2)
                            
            except Exception:
                pass
    
    return parts_list

def detect_all_devices(usb_ports):
    all_devices_found = []
    for port in usb_ports:
        found_on_port = detect_comm_details(port)
        all_devices_found.extend(found_on_port)
    
    return all_devices_found

if __name__ == "__main__":
    usb_ports_to_scan = ["/dev/ttyUSB0", "/dev/ttyUSB1"]
    all_found_devices = detect_all_devices(usb_ports_to_scan)
    if all_found_devices:
        for dev in all_found_devices:
            print(dev)
    else:
        print("No devices were detected on the specified ports.")