import sys
try:
    from pymodbus.client import ModbusTcpClient
    from pymodbus.client import ModbusSerialClient
    #from pymodbus import Framer
except Exception as e:
    from pymodbus.client.sync import ModbusSerialClient
    from pymodbus.client.sync import ModbusTcpClient

from pymodbus.server import StartSerialServer,StartTcpServer
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusSlaveContext, ModbusServerContext
import enum
import threading
import json

class modbusType(enum.IntEnum):

    tcp=0,
    rtu=1

    @classmethod
    def from_param(cls, obj):
        return int(obj)

class slaveTCPdetails(object):
    ip : str 
    port:int 

    def __init__(self,ip,port) -> None:
        self.ip = ip
        self.port = port
        pass



class slaveRTUdetails(object):
    port : str
    parity:str
    baudrate:int
    id : int
    slave_context :ModbusSlaveContext
    storeir : ModbusSequentialDataBlock
    server_context = ModbusServerContext
    
    def __init__(self,port : str,parity : str,baud :int,id) -> None:
        self.port = port
        self.parity = parity
        self.baudrate = baud
        self.id = id

        
        pass



        
identity = ModbusDeviceIdentification(
    info=None
)


class modbusSlaveDevice():
    comm_type : modbusType
    tcp_details : slaveTCPdetails
    rtu_details : slaveRTUdetails
 

    def __init__(self) -> None:
        f = open("modbus_slave_cfg.json","r")
        config = json.load(f)
        self.comm_type = getattr(modbusType,config["type"])
        #get tcp details : 
        if("tcp_details") in config:
            self.tcp_details = slaveTCPdetails(config["tcp_details"]["ip"],config["tcp_details"]["port"])

        if("rtu_details") in config:
            self.rtu_details = slaveRTUdetails(config["rtu_details"]["port"],config["rtu_details"]["parity"],config["rtu_details"]["baud"],config["rtu_details"]["slave_id"])
        pass

        self.storeir = ModbusSequentialDataBlock(0x00,[0]*50000)
        self.slave_context= ModbusSlaveContext(ir=self.storeir)
        self.server_context = ModbusServerContext(slaves={self.rtu_details.id : self.slave_context},single=False)

    def runRTUServer(self):
        StartSerialServer(
            context = self.server_context,
            identity = identity,
            port = self.rtu_details.port,
            parity = self.rtu_details.parity,
            baudrate=self.rtu_details.baudrate,
        )

    def runTCPServer(self):
        StartTcpServer(
            context=self.server_context,
            identity=identity,
            address = (self.tcp_details.ip,self.tcp_details.port)
        )
    
    def updateData(self):
        #read data from iomaster and write to corresponding registers
        pass


if __name__ == "__main__":
    device = modbusSlaveDevice()
    t1 = threading.Thread(target=device.runTCPServer)
    t1.start()
    t1.join()
["rtu_details"]
    

    #def getPortDetails():


