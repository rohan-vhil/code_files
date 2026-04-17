import sys
try:
    from pymodbus.client import ModbusTcpClient
    from pymodbus.client import ModbusSerialClient
    from pymodbus.exceptions import ModbusIOException
except Exception as e:
    from pymodbus.client.sync import ModbusSerialClient
    from pymodbus.client.sync import ModbusTcpClient
    from pymodbus.exceptions import ModbusIOException

from datetime import datetime, time as dtime
import enum
import random
import time
import socket
import struct
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
                print(f"---- Attempting TCP connection to {self.modbusTCP_comm_details.ip}:{self.modbusTCP_comm_details.port} ----")
                logging.info(f"Attempting to connect to TCP device at {self.modbusTCP_comm_details.ip}:{self.modbusTCP_comm_details.port}")
                self.device_connected = self.mbus_client.connect()
                if not self.device_connected:
                    print(f"---- TCP connection failed to {self.modbusTCP_comm_details.ip} ----")
                    logging.warning(f"Connection failed to {self.modbusTCP_comm_details.ip}")
                else:
                    print(f"---- TCP connection successful to {self.modbusTCP_comm_details.ip} ----")
            except Exception as e:
                print(f"---- TCP connection exception: {e} ----")
                logging.error(f"TCP connection exception: {e}")
                self.device_connected = False
        else:
            self.device_connected = True
        return self.device_connected

    def close_connection(self):
        print(f"---- Closing TCP connection for {self.modbusTCP_comm_details.ip} ----")
        try:
            self.mbus_client.close()
        except Exception:
            pass
        self.device_connected = False
        logging.info(f"Connection closed for TCP device {self.modbusTCP_comm_details.ip}")
        return False
        
    def hard_reset(self):
        print(f"---- OS-LEVEL HARD RESET triggered for TCP device {self.modbusTCP_comm_details.ip} ----")
        try:
            if hasattr(self.mbus_client, 'socket') and self.mbus_client.socket:
                # Set SO_LINGER to 0 to send a TCP RST, instantly killing the zombie connection
                self.mbus_client.socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                self.mbus_client.socket.close()
        except Exception:
            pass
            
        try:
            self.mbus_client.close()
        except Exception:
            pass
            
        self.device_connected = False
        time.sleep(1.0) 
        self.mbus_client = ModbusTcpClient(self.modbusTCP_comm_details.ip, port=self.modbusTCP_comm_details.port)

    def writeDataToRegisters(self, reg_data_list,addr):
        try:
            if not self.mbus_client.is_socket_open():
                self.connect()
            
            if self.device_connected:
                print(f"Device ID: {getattr(self, 'device_id', 'N/A')}, Writing to Register: {addr}, Data: {reg_data_list}")
                self.mbus_client.write_registers(addr, reg_data_list, slave=self.slave_id)
                time.sleep(1.0)
            else:
                logging.warning(f"Unable to write data: device {self.modbusTCP_comm_details.ip} is not connected.")
        except ModbusIOException as e:
            logging.error(f"Modbus write error: {e}")
            self.hard_reset()
        except Exception as e:
            logging.error(f"Unable to write data due to exception: {e}")
            self.hard_reset()

    def writeCoilStatus(self, coil_data):
        try:
            if not self.mbus_client.is_socket_open():
                self.connect()

            if self.device_connected:
                print(f"Device ID: {getattr(self, 'device_id', 'N/A')}, Writing Coil to Address: {coil_data['address']}, Value: {coil_data['value']}")
                self.mbus_client.write_coil(coil_data["address"], coil_data["value"], slave=self.slave_id)
                time.sleep(1.0)
            else:
                logging.warning(f"Unable to write coil status: device {self.modbusTCP_comm_details.ip} is not connected.")
        except KeyError as e:
            logging.error(f"Missing key in coil data dictionary: {e}")
        except ModbusIOException as e:
            logging.error(f"Modbus write error: {e}")
            self.hard_reset()
        except Exception as e:
            logging.error(f"Unable to write coil status due to exception: {e}")
            self.hard_reset()

    def writeDataToCtrlRegisters(self, reg_data):
        try:
            if not self.mbus_client.is_socket_open():
                self.connect()

            if self.device_connected:
                builder = BinaryPayloadBuilder(byteorder=getattr(Endian, reg_data["bo"]), wordorder=getattr(Endian, reg_data["wo"]))
                attribute = getattr(builder, reg_data["format"])
                attribute(int(reg_data["value"]))
                payload = builder.build()
                print(f"Device ID: {getattr(self, 'device_id', 'N/A')}, Writing Control Register: {reg_data['address']}, Value: {reg_data['value']}, Payload: {payload[0]}")
                self.mbus_client.write_register(
                        reg_data["address"], payload[0], skip_encode=True, slave=self.slave_id
                    )
                time.sleep(1.0)
            else:
                logging.warning(f"Unable to write control data: device {self.modbusTCP_comm_details.ip} is not connected.")
        except KeyError as e:
            logging.error(f"Missing key in control data dictionary: {e}")
        except AttributeError as e:
            logging.error(f"Invalid format attribute in control data: {e}")
        except ModbusIOException as e:
            logging.error(f"Modbus write error: {e}")
            self.hard_reset()
        except Exception as e:
            logging.error(f"Unable to write control data due to exception: {e}")
            self.hard_reset()

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
            self.mbus_client = ModbusSerialClient(method="rtu", port=self.modbusRTU_comm_details.port, baudrate=baud, timeout=3, parity=parity)
        else:
            self.mbus_client = ModbusSerialClient(port, framer.FramerType.RTU, baud, 8, parity, stop_bits, timeout=3)
        self.device_connected = False

    def connect(self):
        try:
            print(f"---- Attempting RTU connection to {self.modbusRTU_comm_details.port} ----")
            logging.info(f"Attempting to connect to RTU device at {self.modbusRTU_comm_details.port}")
            time.sleep(0.05)  
            self.device_connected = self.mbus_client.connect()
            
            if self.device_connected:
                print(f"---- RTU connection successful on {self.modbusRTU_comm_details.port}. Proceeding to flush buffers... ----")
                if hasattr(self.mbus_client, 'socket') and self.mbus_client.socket:
                    try:
                        if hasattr(self.mbus_client.socket, 'reset_input_buffer'):
                            print("---- Using reset_input_buffer() / reset_output_buffer() for flushing ----")
                            self.mbus_client.socket.reset_input_buffer()
                            self.mbus_client.socket.reset_output_buffer()
                        else:
                            print("---- Using flushInput() / flushOutput() for flushing ----")
                            self.mbus_client.socket.flushInput()
                            self.mbus_client.socket.flushOutput()
                        print("---- Serial port buffers flushed successfully ----")
                        logging.debug("Serial port buffers flushed successfully.")
                    except Exception as e:
                        print(f"---- Exception during buffer flush: {e} ----")
                        logging.debug(f"Could not flush serial buffers: {e}")
            else:
                print(f"---- RTU connection failed to {self.modbusRTU_comm_details.port} ----")
                logging.warning(f"Connection failed to {self.modbusRTU_comm_details.port}")

        except Exception as e:
            print(f"---- RTU connection exception: {e} ----")
            logging.error(f"RTU connection exception: {e}")
            self.device_connected = False
        return self.device_connected

    def close_connection(self):
        if self.mbus_client.is_socket_open():
            print(f"---- Closing RTU connection for {self.modbusRTU_comm_details.port} ----")
            self.mbus_client.close()
            self.device_connected = False
            logging.info(f"Connection closed for RTU device {self.modbusRTU_comm_details.port}")
        return False

    def hard_reset(self):
        print(f"---- OS-LEVEL HARD RESET triggered for RTU device on {self.modbusRTU_comm_details.port} ----")
        try:
            # Aggressively flush the hardware buffers before closing
            if hasattr(self.mbus_client, 'socket') and self.mbus_client.socket:
                try:
                    if hasattr(self.mbus_client.socket, 'reset_input_buffer'):
                        self.mbus_client.socket.reset_input_buffer()
                        self.mbus_client.socket.reset_output_buffer()
                    else:
                        self.mbus_client.socket.flushInput()
                        self.mbus_client.socket.flushOutput()
                except Exception:
                    pass
            self.mbus_client.close()
        except Exception:
            pass
            
        self.device_connected = False
        # Give the Linux kernel udev manager 1.5 seconds to fully release the /dev/ttyUSB lock
        time.sleep(1.5) 
        
        # Instantiate a completely fresh client to wipe corrupted framer states
        if sys.version_info.major < 3 or sys.version_info.minor < 10:
            self.mbus_client = ModbusSerialClient(method="rtu", port=self.modbusRTU_comm_details.port, baudrate=self.modbusRTU_comm_details.baud, timeout=3, parity=self.modbusRTU_comm_details.parity)
        else:
            self.mbus_client = ModbusSerialClient(self.modbusRTU_comm_details.port, framer.FramerType.RTU, self.modbusRTU_comm_details.baud, 8, self.modbusRTU_comm_details.parity, self.modbusRTU_comm_details.stop_bits, timeout=3)
        print(f"---- RTU Hard Reset Complete for {self.modbusRTU_comm_details.port} ----")

    def writeDataToRegisters(self, reg_data_list,addr):
        print("----into writeDataToRegisters in modbusRTUDevice----")
        try:
            if not self.mbus_client.is_socket_open():
                self.connect()

            if self.device_connected:
                print(f"Device ID: {getattr(self, 'device_id', 'N/A')}, Writing to Register: {addr}, Data: {reg_data_list}")
                self.mbus_client.write_registers(addr, reg_data_list, slave=self.slave_id)
                time.sleep(1.0) 
            else:
                logging.warning(f"Unable to write data: RTU device on port {self.modbusRTU_comm_details.port} is not connected.")
        except ModbusIOException as e:
            logging.error(f"Modbus write error: {e}")
            self.hard_reset()
        except Exception as e:
            logging.error(f"Unable to write data due to exception: {e}")
            self.hard_reset()

    def writeCoilStatus(self, coil_data):
        try:
            if not self.mbus_client.is_socket_open():
                self.connect()

            if self.device_connected:
                print(f"Device ID: {getattr(self, 'device_id', 'N/A')}, Writing Coil to Address: {coil_data['address']}, Value: {coil_data['value']}")
                self.mbus_client.write_coil(coil_data["address"], coil_data["value"], slave=self.slave_id)
                time.sleep(1.0)
            else:
                logging.warning(f"Unable to write coil status: device {self.modbusRTU_comm_details.port} is not connected.")
        except KeyError as e:
            logging.error(f"Missing key in coil data dictionary: {e}")
        except ModbusIOException as e:
            logging.error(f"Modbus write error: {e}")
            self.hard_reset()
        except Exception as e:
            logging.error(f"Unable to write coil status due to exception: {e}")
            self.hard_reset()

    def writeDataToCtrlRegisters(self, reg_data):
        print("----into writeDataToCtrlRegisters----")
        try:
            if not self.mbus_client.is_socket_open():
                self.connect()

            if self.device_connected:
                builder = BinaryPayloadBuilder(byteorder=getattr(Endian, reg_data["bo"]), wordorder=getattr(Endian, reg_data["wo"]))
                attribute = getattr(builder, reg_data["format"])
                attribute(int(reg_data["value"]))
                payload = builder.build()
                print(f"Device ID: {getattr(self, 'device_id', 'N/A')}, Writing Control Register: {reg_data['address']}, Value: {reg_data['value']}, Payload: {payload[0]}")
                self.mbus_client.write_register(reg_data["address"], payload[0], skip_encode=True, slave=self.slave_id)
                time.sleep(1.0)
            else:
                logging.warning(f"Unable to write control data: RTU device on port {self.modbusRTU_comm_details.port} is not connected.")
        except KeyError as e:
            logging.error(f"Missing key in control data dictionary: {e}")
        except AttributeError as e:
            logging.error(f"Invalid format attribute in control data: {e}")
        except ModbusIOException as e:
            logging.error(f"Modbus write error: {e}")
            self.hard_reset()
        except Exception as e:
            logging.error(f"Unable to write control data due to exception: {e}")
            self.hard_reset()


def bytes_to_registers(data, byteorder="little"):
    print("----into byte_to_registers----")
    registers = []
    try:
        for x in data:
            for i in range(0, len(x), 2):
                register = int.from_bytes(x[i : i + 2], byteorder=byteorder)
                registers.append(register)
    except Exception as e:
        logging.error(f"Error converting bytes to registers: {e}")
        return []
    print("registers after bytes_to_registers : ",registers)
    return registers


def writeModbusData(device: Union[modbusRTUDevice, modbusTCPDevice], address, data, byteorder="little"):
    print("----into writeModbusData----")
    payload = []
    try:
        payload = bytes_to_registers(data, byteorder)
        print(f"Device ID: {getattr(device, 'device_id', 'N/A')}, Writing to Register: {address}, Payload: {payload}")
        device.mbus_client.write_registers(address, values=payload, slave=device.slave_id)
    except ModbusIOException as e:
        logging.error(f"Modbus write error: {e}")
    except Exception as e:
        logging.error(f"Unable to write data due to exception: {e}")
    finally:
        if isinstance(device, modbusRTUDevice):
            time.sleep(1.0)
            device.close_connection()
        elif isinstance(device, modbusTCPDevice):
            time.sleep(1.0)


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
                read_func = device.mbus_client.read_discrete_inputs
            elif reg_type == "co":
                read_func = device.mbus_client.read_coils
            else:
                logging.warning(f"Unknown register type: {reg_type}. Skipping block '{block}'.")
                continue
            
            data.append([])
            
            def robust_read(addr, chunk_size):
                for attempt in range(4):
                    val = read_func(addr, chunk_size, slave=device.slave_id)
                    if not val.isError():
                        if reg_type in ["ir", "hr"] and hasattr(val, 'registers'):
                            return val
                        elif reg_type in ["di", "co"] and hasattr(val, 'bits'):
                            return val
                    
                    time.sleep(0.5)
                    
                    if isinstance(device, modbusRTUDevice) and hasattr(device.mbus_client, 'socket') and device.mbus_client.socket:
                        try:
                            if hasattr(device.mbus_client.socket, 'reset_input_buffer'):
                                device.mbus_client.socket.reset_input_buffer()
                            else:
                                device.mbus_client.socket.flushInput()
                        except Exception:
                            pass
                    elif isinstance(device, modbusTCPDevice) and hasattr(device.mbus_client, 'socket') and device.mbus_client.socket:
                        try:
                            device.mbus_client.socket.setblocking(0)
                            device.mbus_client.socket.recv(4096)
                            device.mbus_client.socket.setblocking(1)
                        except Exception:
                            pass
                return val

            while remaining_length > MAX_LENGTH:
                val = robust_read(start_addr, MAX_LENGTH)
                if val.isError() or (reg_type in ["ir", "hr"] and not hasattr(val, 'registers')) or (reg_type in ["di", "co"] and not hasattr(val, 'bits')):
                    raise ModbusIOException(f"Expected read response but got {type(val)} at address {start_addr}")
                
                if reg_type in ["ir", "hr"]:
                    data[-1] += val.registers
                elif reg_type in ["di", "co"]:
                    data[-1] += val.bits
                    
                remaining_length -= MAX_LENGTH
                start_addr += MAX_LENGTH

            val = robust_read(start_addr, remaining_length)
            if val.isError() or (reg_type in ["ir", "hr"] and not hasattr(val, 'registers')) or (reg_type in ["di", "co"] and not hasattr(val, 'bits')):
                raise ModbusIOException(f"Expected read response but got {type(val)} at address {start_addr}")

            if reg_type in ["ir", "hr"]:
                data[-1] += val.registers
            elif reg_type in ["di", "co"]:
                data[-1] += val.bits

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
        logging.error(f"An error occurred during data read for device {getattr(device, 'device_id', 'N/A')}: {e}. Triggering hard reset.")
        device.hard_reset()
        modbusdata['read'] = []
        modbusdata['control'] = []
    
    finally:
        if device.device_connected and isinstance(device, modbusRTUDevice):
            device.close_connection()

    return modbusdata