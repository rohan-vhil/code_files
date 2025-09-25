import os
import sys
from pymodbus.client import ModbusTcpClient, ModbusSerialClient
from pymodbus.client import ModbusBaseClient
from pymodbus import framer
from pymodbus.exceptions import ModbusException
from dataclasses import dataclass
import json
import time

# Use relative path for modules
sys.path.insert(0, "../")

# Define the serial port path. Using a constant for clarity.
# Using /dev/serial/by-id/ is a good practice for persistent device naming.
SERIAL_PATH = "/dev/serial/by-id/"

# Pre-defined connection parameters
BAUDRATES = [9600, 4800, 19200]
PARITIES = ['O', 'N', 'E']
SLAVES = [1, 2, 3, 4, 5]

@dataclass
class ModbusDevice:
    """Dataclass to hold detected Modbus device information."""
    port: str
    baud: int
    parity: str
    slave_id: int
    device_name: str

def find_parameter(device_name: str, parameter: str, json_path="modbus_registers.json") -> dict | None:
    """
    Finds a specific parameter's register information for a given device.
    
    Args:
        device_name: The name of the device (e.g., "growatt").
        parameter: The name of the parameter (e.g., "L1_voltage").
        json_path: Path to the JSON file containing Modbus register maps.

    Returns:
        A dictionary with parameter details (address, size, etc.) or None if not found.
    """
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: JSON file not found at {json_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from {json_path}")
        return None

    device_info = data.get(device_name)
    if not device_info:
        return None
    
    for block in device_info.values():
        param_data = block.get("data", {})
        if parameter in param_data:
            result = param_data[parameter].copy()
            # Combine block and parameter info for a complete picture
            result.update({
                "start_address": block.get("start_address"),
                "registers": block.get("registers")
            })
            return result
    return None

def verify_part(client: ModbusBaseClient, device_name: str, slave_id: int, cfg: dict, maps: dict) -> bool:
    """
    Verifies if a specific device is present by reading key parameters.
    
    Args:
        client: An initialized Modbus client (Serial or TCP).
        device_name: The name of the device to verify.
        slave_id: The Modbus slave ID of the device.
        cfg: The auto_config_details dictionary.
        maps: The modbus_registers dictionary.

    Returns:
        True if the device is verified, False otherwise.
    """
    device_registers = maps.get(device_name)
    if not device_registers:
        print(f"Error: Device '{device_name}' not found in modbus_registers.json")
        return False
        
    try:
        device_params = cfg["devices"][device_name]
        input_params = device_params["input"]["param_list"]
        holding_params = device_params["holding"]["param_list"]
    except KeyError as e:
        print(f"Error: Missing key in auto_config_details.json for device '{device_name}': {e}")
        return False

    # Check both input and holding registers
    for param_list, register_type in [(input_params, "input"), (holding_params, "holding")]:
        for param in param_list:
            param_info = find_parameter(device_name, param, json_path="modbus_registers.json")
            if not param_info:
                print(f"Could not find parameter '{param}' info for '{device_name}'.")
                return False

            try:
                # The address calculation should be robust
                address = param_info.get("start_address", 0) + param_info.get("offset", 0)
                num_registers = param_info.get("size", 1)

                if register_type == "input":
                    regs = client.read_input_registers(address=address, count=num_registers, slave=slave_id)
                else: # holding
                    regs = client.read_holding_registers(address=address, count=num_registers, slave=slave_id)

                if regs.isError():
                    print(f"Error reading {register_type} register for '{param}' at address {address}: {regs}")
                    return False
                
                # Default to 1 if m_f is "NA"
                multiplier = param_info.get("m_f", 1)
                if multiplier == "NA":
                    multiplier = 1

                # Correctly handle data conversion
                try:
                    data_format = param_info.get("format", "UINT16")
                    # Use client's own conversion method for simplicity and compatibility
                    data_value = client.convert_from_registers(regs.registers, getattr(client.DATATYPE, data_format)) * multiplier
                except AttributeError:
                    print(f"Error: Invalid data format '{data_format}' for parameter '{param}'.")
                    return False
                
                # Check if the read value is within the expected range
                min_val, max_val = cfg["param_range"].get(param, [float('-inf'), float('inf')])
                if not (min_val <= data_value <= max_val):
                    print(f"Value for '{param}' ({data_value}) is out of expected range [{min_val}, {max_val}].")
                    return False
                
                print(f"Successfully read and validated '{param}' ({data_value}) from address {address}.")
            
            except ModbusException as e:
                print(f"Modbus exception during read for '{param}': {e}")
                return False
            except Exception as e:
                print(f"Unexpected exception during read for '{param}': {e}")
                return False
    
    return True

def detect_comm_details(port: str, device_nums: int = 1) -> list[ModbusDevice]:
    """
    Detects and identifies Modbus devices on a given serial port.
    
    Args:
        port: The serial port to scan (e.g., "/dev/ttyUSB0").
        device_nums: The number of devices to detect before stopping.

    Returns:
        A list of ModbusDevice objects found.
    """
    parts_list = []
    devices_found = 0
    
    try:
        with open("auto_config_details.json", 'r') as cfg_file:
            cfg = json.load(cfg_file)
        with open("modbus_registers.json", 'r') as modbus_file:
            maps = json.load(modbus_file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading configuration files: {e}")
        return []

    for baud in BAUDRATES:
        for par in PARITIES:
            # Use a 'try...finally' block to ensure client.close() is called
            client = None
            try:
                # Use a smaller timeout for faster scans, adjust as needed
                client = ModbusSerialClient(port=port, baudrate=baud, parity=par, framer=framer.FramerType.RTU, timeout=0.5)
                
                if not client.connect():
                    print(f"Could not connect to port {port} with baud {baud}, parity {par}. Skipping.")
                    continue

                for slave_id in SLAVES:
                    for device_name in cfg["devices"]:
                        print(f"Checking for device '{device_name}' at slave ID {slave_id} on {port}...")
                        
                        if verify_part(client, device_name, slave_id, cfg, maps):
                            print(f"🎉 Found device '{device_name}' at slave ID {slave_id} on {port}!")
                            devices_found += 1
                            parts_list.append(ModbusDevice(port=port, baud=baud, parity=par, slave_id=slave_id, device_name=device_name))
                            
                            # Check if we've found enough devices
                            if devices_found >= device_nums:
                                return parts_list
                        else:
                            print(f"Device '{device_name}' not found at slave ID {slave_id}.")
                        
                        # Add a short delay to prevent flooding the bus
                        time.sleep(0.2)
                        
            except Exception as e:
                print(f"An error occurred during detection on port {port}: {e}")
            finally:
                if client and client.is_auto_connect:
                    client.close()
    
    return parts_list

def detect_all_devices(directory: str = SERIAL_PATH, device_nums: int = 1) -> list[ModbusDevice]:
    """
    Scans all serial ports in a directory for Modbus devices.
    
    Args:
        directory: The directory to scan for serial devices.
        device_nums: The number of devices to detect on each port.
    
    Returns:
        A list of all detected ModbusDevice objects.
    """
    try:
        files = os.listdir(directory)
    except FileNotFoundError:
        print(f"Error: Serial port directory '{directory}' not found.")
        return []
    
    all_devices_found = []
    for file in files:
        full_path = os.path.join(directory, file)
        print(f"\n--- Scanning port: {full_path} ---")
        found_on_port = detect_comm_details(full_path, device_nums)
        all_devices_found.extend(found_on_port)
    
    return all_devices_found

if __name__ == "__main__":
    # Example 1: Scan a single, specific port for up to 2 devices
    print("--- Starting scan on /dev/ttyUSB0 ---")
    devices = detect_comm_details("/dev/ttyUSB0", 2)
    for device in devices:
        print(device)
    
    print("\n" + "="*50 + "\n")
    
    # Example 2: Scan all detected serial ports for 1 device each
    # This assumes your /dev/serial/by-id/ directory is populated
    print(f"--- Starting full scan on all ports in {SERIAL_PATH} ---")
    all_found_devices = detect_all_devices()
    if all_found_devices:
        print("\n--- All detected devices: ---")
        for dev in all_found_devices:
            print(dev)
    else:
        print("No devices were detected on any serial ports.")