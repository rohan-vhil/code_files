'''Modbus API Code Review and Update
For hr (Holding Registers) and ir (Input Registers), the raw data will be a list of 16-bit integers (register values).

For di (Discrete Inputs) and co (Coils), the raw data will be a list of booleans (True/False)'''


import sys
try:
    from pymodbus.client import ModbusTcpClient
    from pymodbus.client import ModbusSerialClient
    from pymodbus.exceptions import ModbusIOException
except Exception as e:
    from pymodbus.client.sync import ModbusSerialClient
    from pymodbus.client.sync import ModbusTcpClient
    from pymodbus.exceptions import ModbusIOException

from datetime import datetime, time
import enum
import random
import time
import sys

sys.path.insert(0, "../")
sys.path.insert(0,'../control/')
from control import control_base as ctrl

from pymodbus.pdu import ModbusExceptions as mexcpt
from pymodbus.pdu import ExceptionResponse as mbusresp
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.constants import Endian
from pymodbus import framer

from typing import Union
import logging


class modbusTCPDetails:
    ip = None
    port = None
    address_map = None
    slave_id = 0


class modbusRTUdetails:
    port = None
    slave_id = 0
    address_map = None
    parity = 1
    stop_bits = 1
    baud = 9600


class read_result(enum.IntEnum):
    success = 0,
    fail_retry = 1,
    fail_retry_later = 2,
    fail_move = 3,

    @classmethod
    def from_param(cls, obj):
        return int(obj)


class modbusRegType(enum.IntEnum):
    input = 0,
    holding = 1,
    discrete_input = 2,
    coil = 3

    @classmethod
    def from_param(cls, obj):
        return int(obj)


class mbusErrorCodes(enum.IntEnum):
    no_error = 0,
    connection_error = 1,
    illegalAdrress = 2,
    illegalValue = 3,
    noresponse = 4,

    @classmethod
    def from_param(cls, obj):
        return int(obj)


class modbusTCPDevice(ctrl.systemDevice):
    modbusTCP_comm_details: modbusTCPDetails
    mbus_client: ModbusTcpClient
    device_connected: bool
    
    def __init__(self, devicetype, commtype,ip, port,slave_id=1,address_map={},ctrl_map={},cfg={}) -> None:
        super().__init__(devicetype, commtype,cfg)
        self.modbusTCP_comm_details = modbusTCPDetails()
        self.device_connected = False
        self.modbusTCP_comm_details.ip = ip
        self.modbusTCP_comm_details.port = port
        self.port = port
        self.slave_id = slave_id
        self.addr_map = address_map
        self.ctrl_map = ctrl_map
        self.mbus_client = ModbusTcpClient(ip, port=port)
        print("port is : ", self.modbusTCP_comm_details.port)

    def connect(self):
        if not self.mbus_client.is_socket_open():
            try:
                logging.info(f"Attempting to connect to TCP device at {self.modbusTCP_comm_details.ip}:{self.modbusTCP_comm_details.port}")
                self.device_connected = self.mbus_client.connect()
                if not self.device_connected:
                    logging.warning(f"Connection failed to {self.modbusTCP_comm_details.ip}")
            except Exception as e:
                logging.error(f"TCP connection exception: {e}")
                self.device_connected = False
        else:
            self.device_connected = True
        return self.device_connected

    def close_connection(self):
        if self.mbus_client.is_socket_open():
            self.mbus_client.close()
            self.device_connected = False
            logging.info(f"Connection closed for TCP device {self.modbusTCP_comm_details.ip}")
        return False

    def writeDataToRegisters(self, reg_data_list,addr):
            try:
                if not self.mbus_client.is_socket_open():
                    self.connect()
                
                if self.device_connected:
                    self.mbus_client.write_registers(addr, reg_data_list)
                else:
                    logging.warning(f"Unable to write data: device {self.modbusTCP_comm_details.ip} is not connected.")
            except ModbusIOException as e:
                logging.error(f"Modbus write error: {e}")
                self.close_connection()
            except Exception as e:
                logging.error(f"Unable to write data due to exception: {e}")
                self.close_connection()

    def writeDataToCtrlRegisters(self, reg_data):
        try:
            if not self.mbus_client.is_socket_open():
                self.connect()

            if self.device_connected:
                if reg_data["type"] == "co":
                    self.mbus_client.write_coil(reg_data["address"], reg_data["value"], slave=self.slave_id)
                else:
                    builder = BinaryPayloadBuilder(byteorder=getattr(Endian, reg_data["bo"]), wordorder=getattr(Endian, reg_data["wo"]))
                    attribute = getattr(builder, reg_data["format"])
                    attribute(int(reg_data["value"]))
                    payload = builder.build()
                    self.mbus_client.write_register(
                            reg_data["address"], payload[0], skip_encode=True, slave=self.slave_id
                        )
            else:
                logging.warning(f"Unable to write control data: device {self.modbusTCP_comm_details.ip} is not connected.")
        except KeyError as e:
            logging.error(f"Missing key in control data dictionary: {e}")
        except AttributeError as e:
            logging.error(f"Invalid format attribute in control data: {e}")
        except ModbusIOException as e:
            logging.error(f"Modbus write error: {e}")
            self.close_connection()
        except Exception as e:
            logging.error(f"Unable to write control data due to exception: {e}")
            self.close_connection()

class modbusRTUDevice(ctrl.systemDevice):
    modbusRTU_comm_details: modbusRTUdetails
    mbus_client: ModbusSerialClient
    addr_map: dict

    def __init__(
        self, devicetype, commtype, address_map, control_map, port, parity, stop_bits, baud, slave_id=0, rated_power=15000,cfg={}) -> None:
        super().__init__(devicetype, commtype,cfg)
        self.modbusRTU_comm_details = modbusRTUdetails()
        self.addr_map = address_map
        self.ctrl_map = control_map
        self.modbusRTU_comm_details.port = port
        self.modbusRTU_comm_details.parity = parity
        self.modbusRTU_comm_details.baud = baud
        self.modbusRTU_comm_details.slave_id = slave_id
        self.slave_id = slave_id
        self.modbusRTU_comm_details.stop_bits = stop_bits
        if sys.version_info.major < 3 or sys.version_info.minor < 10:
            self.mbus_client = ModbusSerialClient(method="rtu", port=self.modbusRTU_comm_details.port, baudrate=baud, timeout=1, parity=parity)
        else:
            self.mbus_client = ModbusSerialClient(port, framer.FramerType.RTU, baud, 8, parity, stop_bits)
        self.device_connected = False

    def connect(self):
        try:
            logging.info(f"Attempting to connect to RTU device at {self.modbusRTU_comm_details.port}")
            self.device_connected = self.mbus_client.connect()
            if not self.device_connected:
                logging.warning(f"Connection failed to {self.modbusRTU_comm_details.port}")
        except Exception as e:
            logging.error(f"RTU connection exception: {e}")
            self.device_connected = False
        return self.device_connected

    def close_connection(self):
        if self.mbus_client.is_socket_open():
            self.mbus_client.close()
            self.device_connected = False
            logging.info(f"Connection closed for RTU device {self.modbusRTU_comm_details.port}")
        return False

    def writeDataToRegisters(self, reg_data_list,addr):
        try:
            self.mbus_client.write_registers(addr, reg_data_list)
        except ModbusIOException as e:
            logging.error(f"Modbus write error: {e}")
            self.close_connection()
        except Exception as e:
            logging.error(f"Unable to write data due to exception: {e}")

    def writeDataToCtrlRegisters(self, reg_data):
        try:
            if reg_data["type"] == "co":
                self.mbus_client.write_coil(reg_data["address"], reg_data["value"], slave=self.slave_id)
            else:
                builder = BinaryPayloadBuilder(byteorder=getattr(Endian, reg_data["bo"]), wordorder=getattr(Endian, reg_data["wo"]))
                attribute = getattr(builder, reg_data["format"])
                attribute(int(reg_data["value"]))
                payload = builder.build()
                self.mbus_client.write_register(reg_data["address"], payload[0], skip_encode=True, unit=0, slave=0)
        except KeyError as e:
            logging.error(f"Missing key in control data dictionary: {e}")
        except AttributeError as e:
            logging.error(f"Invalid format attribute in control data: {e}")
        except ModbusIOException as e:
            logging.error(f"Modbus write error: {e}")
            self.close_connection()
        except Exception as e:
            logging.error(f"Unable to write control data due to exception: {e}")
            self.close_connection()


def bytes_to_registers(data, byteorder="little"):
    registers = []
    try:
        for x in data:
            for i in range(0, len(x), 2):
                register = int.from_bytes(x[i : i + 2], byteorder=byteorder)
                registers.append(register)
    except Exception as e:
        logging.error(f"Error converting bytes to registers: {e}")
        return []
    return registers


def writeModbusData(device: Union[modbusRTUDevice, modbusTCPDevice], address, data, byteorder="little"):
    payload = []
    try:
        payload = bytes_to_registers(data, byteorder)
        device.mbus_client.write_registers(address, values=payload, slave=device.slave_id, unit=device.slave_id)
    except ModbusIOException as e:
        logging.error(f"Modbus write error: {e}")
        if device.comm_type == ctrl.commType.modbus_tcp:
            device.close_connection()
    except Exception as e:
        logging.error(f"Unable to write data due to exception: {e}")


def getData(addrmap:dict,device:Union[modbusRTUDevice, modbusTCPDevice]):
    data = []
    MAX_LENGTH = 125
    try:
        for block in addrmap:
            length = addrmap[block]["Length"]
            start_addr = addrmap[block]["start_address"]
            remaining_length = length
            reg_type = addrmap[block]["registers"]
            
            if reg_type == "ir":
                read_func = device.mbus_client.read_input_registers
            elif reg_type == "hr":
                read_func = device.mbus_client.read_holding_registers
            elif reg_type == "di":
                # Discrete Inputs are single-bit, the pymodbus function is read_discrete_inputs
                read_func = device.mbus_client.read_discrete_inputs
                # For coils/discrete inputs, MAX_LENGTH is 2000, but keeping 125 for safety and consistency with registers.
                # However, for pymodbus 3.7.4, the general register read logic is simpler to maintain.
                # The response object for coils/discrete inputs has a 'bits' attribute, not 'registers'.
            elif reg_type == "co":
                # Coils are single-bit, the pymodbus function is read_coils
                read_func = device.mbus_client.read_coils
            else:
                logging.warning(f"Unknown register type: {reg_type}. Skipping block '{block}'.")
                continue
            
            data.append([])
            while remaining_length > MAX_LENGTH:
                val = read_func(start_addr, MAX_LENGTH, slave=device.slave_id)
                if val.isError():
                    raise ModbusIOException(f"Modbus error: {val} at address {start_addr}")
                
                if reg_type in ["ir", "hr"]:
                    data[-1] += val.registers
                elif reg_type in ["di", "co"]:
                    data[-1] += val.bits
                    
                remaining_length -= MAX_LENGTH
                start_addr += MAX_LENGTH

            val = read_func(start_addr, remaining_length, slave=device.slave_id)
            if val.isError():
                raise ModbusIOException(f"Modbus error: {val} at address {start_addr}")

            if reg_type in ["ir", "hr"]:
                data[-1] += val.registers
            elif reg_type in ["di", "co"]:
                data[-1] += val.bits
            
            if hasattr(device, 'device_id'):
                print(f"Device ID: {device.device_id}, Raw Data Block '{block}': {data[-1]}")
            else:
                print(f"Device ID: N/A, Raw Data Block '{block}': {data[-1]}")

        device.read_error = False
    except ModbusIOException as e:
        device.read_error = True
        raise e
    except Exception as e:
        device.read_error = True
        raise e
        
    return data

def getModbusData(device: Union[modbusRTUDevice, modbusTCPDevice]):
    modbusdata = {'read': [], 'control': []}
    is_tcp = device.comm_type == ctrl.commType.modbus_tcp
    if not is_tcp or not device.mbus_client.is_socket_open():
        if not device.connect():
            logging.warning(f"Failed to connect to device {getattr(device, 'device_id', 'N/A')}. Skipping read cycle.")
            return modbusdata
    try:
        if not device.device_connected:
            logging.error(f"Device {getattr(device, 'device_id', 'N/A')} is not connected after calling connect(). Aborting read.")
            return modbusdata

        modbusdata['read'] = getData(device.addr_map['map'], device)
        modbusdata['control'] = getData(device.ctrl_map['map'], device)
        
    except Exception as e:
        logging.error(f"An error occurred during data read for device {getattr(device, 'device_id', 'N/A')}: {e}. Closing connection.")
        device.close_connection()
        modbusdata['read'] = []
        modbusdata['control'] = []
    
    finally:
        if not is_tcp and device.device_connected:
            device.close_connection()

    return modbusdata