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
    holding = 1

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
        # Only try to connect if the socket is not already open.
        if not self.mbus_client.is_socket_open():
            logging.info(f"Attempting to connect to TCP device at {self.modbusTCP_comm_details.ip}:{self.modbusTCP_comm_details.port}")
            self.device_connected = self.mbus_client.connect()
            if not self.device_connected:
                logging.warning(f"Connection failed to {self.modbusTCP_comm_details.ip}")
        else:
            self.device_connected = True # If socket is open, assume we are connected
        return self.device_connected

    def close_connection(self):
        self.mbus_client.close()
        self.device_connected = False
        logging.info(f"Connection closed for TCP device {self.modbusTCP_comm_details.ip}")
        return False

    def writeDataToRegisters(self, reg_data_list,addr):
            try:
                # Ensure connection before writing
                if not self.mbus_client.is_socket_open():
                    self.connect()
                
                if self.device_connected:
                    self.mbus_client.write_registers(addr, reg_data_list)
                else:
                    logging.warning(f"Unable to write data: device {self.modbusTCP_comm_details.ip} is not connected.")
            except Exception as e:
                logging.warning("Unable to write data "+str(e))
                self.close_connection() # Close connection on write error

    def writeDataToCtrlRegisters(self, reg_data):
        try:
            # Ensure connection before writing
            if not self.mbus_client.is_socket_open():
                self.connect()

            if self.device_connected:
                builder = BinaryPayloadBuilder(byteorder=getattr(Endian, reg_data["bo"]), wordorder=getattr(Endian, reg_data["wo"]))
                attribute = getattr(builder, reg_data["format"])
                print(attribute(int(reg_data["value"])))
                payload = builder.build()
                print(payload)
                print(reg_data["address"])
                x = self.mbus_client.write_register(
                        reg_data["address"], payload[0], skip_encode=True, slave=self.slave_id
                    )
                print(x)
            else:
                logging.warning(f"Unable to write control data: device {self.modbusTCP_comm_details.ip} is not connected.")
        except Exception as e:
            print(e)
            logging.error(str(e))
            self.close_connection() # Close connection on write error

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
        self.device_connected = False # Initial state

    def connect(self):
        # RTU connect must be able to fail and report back.
        # This will attempt to acquire the lock on the serial port.
        logging.info(f"Attempting to connect to RTU device at {self.modbusRTU_comm_details.port}")
        self.device_connected = self.mbus_client.connect()
        if not self.device_connected:
            logging.warning(f"Connection failed to {self.modbusRTU_comm_details.port}")
        return self.device_connected # RETURN ACTUAL CONNECTION STATUS

    def close_connection(self):
        self.mbus_client.close()
        self.device_connected = False
        logging.info(f"Connection closed for RTU device {self.modbusRTU_comm_details.port}")
        return False

    def writeDataToRegisters(self, reg_data_list,addr):
            try:
                # RTU clients often need connect/close around each transaction, but we will
                # rely on getModbusData to manage it to keep a single open connection for a full read cycle.
                # If this write is called outside the read cycle, it assumes the caller will connect/close.
                self.mbus_client.write_registers(addr, reg_data_list)
            except Exception as e:
                logging.warning("unable to write data "+str(e))

    def writeDataToCtrlRegisters(self, reg_data):
        try:
            builder = BinaryPayloadBuilder(byteorder=getattr(Endian, reg_data["bo"]), wordorder=getattr(Endian, reg_data["wo"]))
            attribute = getattr(builder, reg_data["format"])
            print(attribute(int(reg_data["value"])))
            payload = builder.build()
            print(payload)
            print(reg_data["address"])
            x = self.mbus_client.write_register(reg_data["address"], payload[0], skip_encode=True, unit=0, slave=0)
            print(x)
        except Exception as e:
            print(e)
            logging.error(str(e))


def bytes_to_registers(data):
    registers = []
    for x in data:
        for i in range(0, len(x), 2):
            register = int.from_bytes(x[i : i + 2], byteorder="little")
            registers.append(register)
    return registers


def writeModbusData(device: Union[modbusRTUDevice, modbusTCPDevice], address, data):
    payload = []
    try:
        payload = bytes_to_registers(data)
        device.mbus_client.write_registers(address, values=payload, slave=device.slave_id, unit=device.slave_id)
    except Exception as e:
        logging.warning(str(e))
        if device.comm_type == ctrl.commType.modbus_tcp:
            device.close_connection()


def getData(addrmap:dict,device:Union[modbusRTUDevice, modbusTCPDevice]):
    """
    This function performs the raw data reading. It assumes a connection is already established.
    It should not manage the connection state itself.
    """
    data = []
    MAX_LENGTH = 125
    
    # This function now expects the caller (getModbusData) to handle connections.
    # The check for device_connected is moved to the caller.
    try:
        for block in addrmap:
            length = addrmap[block]["Length"]
            start_addr = addrmap[block]["start_address"]
            remaining_length = length
            reg_type = addrmap[block]["registers"]
            
            if reg_type == "ir":
                read_func = device.mbus_client.read_input_registers
            else:
                read_func = device.mbus_client.read_holding_registers
            
            data.append([])
            while remaining_length > MAX_LENGTH:
                val = read_func(start_addr, MAX_LENGTH, slave=device.slave_id)
                if val.isError():
                    raise ModbusIOException(f"Modbus error: {val} at address {start_addr}")
                data[-1] += val.registers
                remaining_length -= MAX_LENGTH
                start_addr += MAX_LENGTH

            val = read_func(start_addr, remaining_length, slave=device.slave_id)
            if val.isError():
                raise ModbusIOException(f"Modbus error: {val} at address {start_addr}")

            data[-1] += val.registers
            
        device.read_error = False
    except Exception as e:
        device.read_error = True
        # Re-raise the exception so the calling function can handle it (e.g., close the connection)
        raise e
        
    return data

def getModbusData(device: Union[modbusRTUDevice, modbusTCPDevice]):
    """
    This is the main data acquisition function. It now manages the connection lifecycle.
    """
    modbusdata = {'read': [], 'control': []}

    is_tcp = device.comm_type == ctrl.commType.modbus_tcp
    
    # --- CONNECTION MANAGEMENT ---
    # For TCP, we only connect if the socket is not already open (to keep connection alive).
    # For RTU, we must connect to acquire the port lock for the transaction.
    # We call connect() if it's RTU, or if it's TCP AND the socket is closed.
    if not is_tcp or not device.mbus_client.is_socket_open():
        # The connect() method is updated to return the true status.
        if not device.connect():
            logging.warning(f"Failed to connect to device {getattr(device, 'device_id', 'N/A')}. Skipping read cycle.")
            return modbusdata

    try:
        # Check connection status after (potentially attempted) connect
        if not device.device_connected:
            logging.error(f"Device {getattr(device, 'device_id', 'N/A')} is not connected after calling connect(). Aborting read.")
            return modbusdata

        # Perform all reads for this cycle
        modbusdata['read'] = getData(device.addr_map['map'], device)
        modbusdata['control'] = getData(device.ctrl_map['map'], device)
        
    except Exception as e:
        logging.error(f"An error occurred during data read for device {getattr(device, 'device_id', 'N/A')}: {e}. Closing connection.")
        # On error, we close the connection for both TCP and RTU.
        device.close_connection()
        # Ensure data is empty on error
        modbusdata['read'] = []
        modbusdata['control'] = []
    
    finally:
        # For RTU devices, we MUST close the serial port after each full transaction 
        # to release the lock for the next process/thread/poll cycle.
        # We only close for RTU here, as we intentionally keep TCP open on success.
        if not is_tcp and device.device_connected:
             device.close_connection()

    return modbusdata