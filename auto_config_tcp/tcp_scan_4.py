'''code by Chatgpt'''

import sys
import json
import time
import socket
import ipaddress
import concurrent.futures
import subprocess

from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple

from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException

TCP_PORTS = [502, 503, 504, 5020]
SLAVES = list(range(1, 61))
RESPONSE_TIMEOUT = 1.0
SHORT_PING_TIMEOUT = 0.3
DELAY_BETWEEN_CHECKS = 0.02
MAX_PORT_SCAN_THREADS = 100
MIN_SUCCESSFUL_PARAMS = 2


@dataclass
class ModbusDevice:
    ip: str
    port: int
    slave_id: int
    device_name: str
    details: Dict[str, Any]


def load_config_files(
    cfg_path="auto_config_details.json",
    maps_path="modbus_registers.json"
) -> Optional[tuple[Dict, Dict]]:

    try:
        with open(cfg_path, 'r') as cfg_file:
            cfg = json.load(cfg_file)

        with open(maps_path, 'r') as modbus_file:
            maps = json.load(modbus_file)

        return cfg, maps

    except Exception:
        return None


def find_parameter(
    device_name: str,
    parameter: str,
    maps: Dict
) -> Optional[Dict]:

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


def read_modbus_register(
    client: ModbusTcpClient,
    slave_id: int,
    param_info: Dict,
    register_type: str
) -> Optional[float]:

    try:

        address = (
            param_info.get("start_address", 0)
            + param_info.get("offset", 0)
        )

        num_registers = param_info.get("size", 1)

        if register_type == "input":

            regs = client.read_input_registers(
                address=address,
                count=num_registers,
                slave=slave_id
            )

        elif register_type == "holding":

            regs = client.read_holding_registers(
                address=address,
                count=num_registers,
                slave=slave_id
            )

        else:
            return None

        if regs is None:
            return None

        if regs.isError():
            return None

        if not hasattr(regs, 'registers'):
            return None

        if not regs.registers:
            return None

        multiplier = param_info.get("m_f", 1)

        if multiplier == "NA":
            multiplier = 1.0

        data_format = param_info.get("format", "UINT16")

        if (
            data_format.startswith('decode_')
            and data_format.endswith('_uint')
        ):
            data_format = 'UINT16'

        value = client.convert_from_registers(
            regs.registers,
            getattr(client.DATATYPE, data_format)
        )

        return value * multiplier

    except Exception:
        return None

    finally:
        time.sleep(DELAY_BETWEEN_CHECKS)


def verify_part(
    client: ModbusTcpClient,
    device_name: str,
    slave_id: int,
    cfg: Dict,
    maps: Dict
) -> bool:

    verification_success_count = 0

    try:

        device_params = cfg["devices"][device_name]

        param_checks = [
            (
                device_params["input"]["param_list"],
                "input"
            ),
            (
                device_params["holding"]["param_list"],
                "holding"
            )
        ]

    except Exception:
        return False

    print(f"  * Verifying '{device_name}' on slave {slave_id}")

    for param_list, register_type in param_checks:

        for param in param_list:

            param_info = find_parameter(
                device_name,
                param,
                maps
            )

            if not param_info:
                continue

            value = read_modbus_register(
                client,
                slave_id,
                param_info,
                register_type
            )

            if value is None:
                continue

            min_val, max_val = cfg["param_range"].get(
                param,
                [float('-inf'), float('inf')]
            )

            if min_val <= value <= max_val:
                verification_success_count += 1
                print(f"    SUCCESS {param} = {value}")

    return verification_success_count >= MIN_SUCCESSFUL_PARAMS


def check_port(
    ip: str,
    port: int,
    timeout: float
) -> Optional[Tuple[str, int]]:

    try:

        with socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        ) as s:

            s.settimeout(timeout)

            if s.connect_ex((ip, port)) == 0:
                return (ip, port)

    except Exception:
        pass

    return None


def scan_subnet_for_ports(
    subnet: str,
    ports: List[int],
    timeout: float = 0.2
) -> List[Tuple[str, int]]:

    active_targets = []

    network = ipaddress.IPv4Network(
        subnet,
        strict=False
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_PORT_SCAN_THREADS
    ) as executor:

        futures = [
            executor.submit(
                check_port,
                str(ip),
                port,
                timeout
            )
            for ip in network.hosts()
            for port in ports
        ]

        for future in concurrent.futures.as_completed(futures):

            result = future.result()

            if result:
                active_targets.append(result)

    return active_targets


def validate_slave(
    ip: str,
    port: int,
    slave_id: int,
    cfg: Dict,
    maps: Dict
) -> bool:

    client = ModbusTcpClient(
        host=ip,
        port=port,
        timeout=SHORT_PING_TIMEOUT
    )

    try:

        if not client.connect():
            return False

        for device_name in cfg["devices"]:

            try:

                device_params = cfg["devices"][device_name]

                for param in device_params["input"]["param_list"]:

                    param_info = find_parameter(
                        device_name,
                        param,
                        maps
                    )

                    if not param_info:
                        continue

                    value = read_modbus_register(
                        client,
                        slave_id,
                        param_info,
                        "input"
                    )

                    if value is not None:
                        return True

                for param in device_params["holding"]["param_list"]:

                    param_info = find_parameter(
                        device_name,
                        param,
                        maps
                    )

                    if not param_info:
                        continue

                    value = read_modbus_register(
                        client,
                        slave_id,
                        param_info,
                        "holding"
                    )

                    if value is not None:
                        return True

            except Exception:
                pass

    except Exception:
        pass

    finally:
        client.close()

    return False


def discover_active_slaves(
    ip: str,
    port: int,
    slave_ids: List[int],
    cfg: Dict,
    maps: Dict
) -> List[int]:

    active_slaves = []

    for slave_id in slave_ids:

        try:

            valid = validate_slave(
                ip,
                port,
                slave_id,
                cfg,
                maps
            )

            if valid:
                active_slaves.append(slave_id)
                print(f"    -> Real slave detected: {slave_id}")

        except Exception:
            pass

    return active_slaves


def get_device_config_details(
    client: ModbusTcpClient,
    device_name: str,
    slave_id: int,
    cfg: Dict,
    maps: Dict
) -> Dict[str, Any]:

    device_details = {}

    try:

        config_list = (
            cfg["devices"][device_name]["holding"]
            ["config_list"]
        )

    except Exception:
        return {}

    for config_param in config_list:

        param_info = find_parameter(
            device_name,
            config_param,
            maps
        )

        if not param_info:
            continue

        register_type = (
            "holding"
            if param_info["registers"] == "hr"
            else "input"
        )

        value = read_modbus_register(
            client,
            slave_id,
            param_info,
            register_type
        )

        if value is not None:
            device_details[config_param] = value

    return device_details


def detect_comm_details(
    ip: str,
    port: int,
    cfg: Dict,
    maps: Dict
) -> List[ModbusDevice]:

    parts_list: List[ModbusDevice] = []

    device_names = list(cfg["devices"].keys())

    print(f"\nScanning: IP={ip}, Port={port}")

    print("  > Discovering active slave IDs...")

    active_slaves = discover_active_slaves(
        ip,
        port,
        SLAVES,
        cfg,
        maps
    )

    if not active_slaves:

        print("  > No valid slave IDs found")

        return parts_list

    print(f"  > Found active slave IDs: {active_slaves}")

    client = ModbusTcpClient(
        host=ip,
        port=port,
        timeout=RESPONSE_TIMEOUT
    )

    try:

        if not client.connect():
            return parts_list

        for slave_id in active_slaves:

            print(
                f"\nScanning: "
                f"IP={ip}, "
                f"Port={port}, "
                f"Slave={slave_id}"
            )

            for device_name in device_names:

                verified = verify_part(
                    client,
                    device_name,
                    slave_id,
                    cfg,
                    maps
                )

                if verified:

                    print(f"\nFOUND DEVICE: {device_name}")

                    details = get_device_config_details(
                        client,
                        device_name,
                        slave_id,
                        cfg,
                        maps
                    )

                    parts_list.append(
                        ModbusDevice(
                            ip=ip,
                            port=port,
                            slave_id=slave_id,
                            device_name=device_name,
                            details=details
                        )
                    )

            time.sleep(0.1)

    finally:
        client.close()

    return parts_list


def get_local_subnets() -> List[str]:

    subnets = []

    try:

        result = subprocess.run(
            ['ip', '-o', '-f', 'inet', 'addr', 'show'],
            capture_output=True,
            text=True,
            check=True
        )

        for line in result.stdout.splitlines():

            parts = line.split()

            if 'inet' in parts:

                idx = parts.index('inet')

                ip_with_cidr = parts[idx + 1]

                if ip_with_cidr.startswith('127.'):
                    continue

                network = (
                    ipaddress.IPv4Interface(
                        ip_with_cidr
                    ).network
                )

                subnets.append(str(network))

    except Exception:
        pass

    return list(set(subnets))


def detect_all_devices(
    subnets: List[str]
) -> List[ModbusDevice]:

    config_data = load_config_files()

    if config_data is None:
        print("Cannot load config files")
        return []

    cfg, maps = config_data

    all_devices_found: List[ModbusDevice] = []

    for subnet in subnets:

        print(f"\n--- Scanning subnet: {subnet} ---")

        active_targets = scan_subnet_for_ports(
            subnet,
            TCP_PORTS
        )

        if not active_targets:
            continue

        print(f"Found active targets: {active_targets}")

        for ip, port in active_targets:

            found_on_target = detect_comm_details(
                ip,
                port,
                cfg,
                maps
            )

            all_devices_found.extend(found_on_target)

    return all_devices_found


if __name__ == "__main__":

    subnets_to_scan = get_local_subnets()

    if not subnets_to_scan:
        print("No active subnets found")
        sys.exit(1)

    print(
        f"--- Starting scan on dynamically "
        f"detected subnets: {subnets_to_scan} ---"
    )

    all_found_devices = detect_all_devices(
        subnets_to_scan
    )

    if all_found_devices:

        print("\n--- All detected devices ---")

        for dev in all_found_devices:
            print(dev)

    else:
        print("\nNo devices detected")