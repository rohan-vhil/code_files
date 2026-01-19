import os
import sys
from pymodbus.client import ModbusTcpClient
from pymodbus.client import ModbusSerialClient
from pymodbus import framer
from dataclasses import dataclass
import json
import time
sys.path.insert(0,"../")
from pymodbus.client import ModbusBaseClient as cltlib

sys.path.insert(0,"/dev")
path = "/dev/serial/by-id/"
#files = [x for x in os.listdir(path)]
#files = os.listdir(path)


#print(files)


bauds =[9600,4800,19200]
num_devices = 1
paritys = ['N']
slaves = [1,2,3,4,5,6,7]
@dataclass
class modbusDEvice:
    port :str
    baud : int
    parity:str
    slave_id : int

"""
device_list = []
for file in files:

    for baud in bauds:
        for parity in paritys:
            for slave in slaves:
                client = ModbusSerialClient(port = path + file,baudrate=baud,parity=parity,framer=framer.FramerType.RTU)
                print(path+file,baud,parity,slave)
                #client.close()
                if(client.connect()):
                    print("connected")
                        #print("connected + ")
                    try:
                        regs = client.read_input_registers(address=1,count=1,slave=slave)
                        print("got response : ",regs.registers,file,slave)
                        if(not regs.isError()):
                                device_list.append(modbusDEvice(path+file,baud,parity))
                        else:
                            print("error")

                    except Exception as e:
                        pass
                        #print("exception : ",e)
                else:
                     print("could not connect to")
                client.close()

print(device_list)
"""

def detectPort(files,device):
    for file in files:
        for baud in bauds:
            for par in paritys:
                client = ModbusSerialClient(port = path + file,baudrate=baud,parity=par,framer=framer.FramerType.RTU)
                for slave in slaves:
                    #iterate through all possibilities
                    
                    if(client.connect()):
                        #connection on port
                        try:
                            #regs = client.read_input_registers(address=1,count=1,slave=slave)
                            #print(regs)
                            
                                #since we are able to connect and read , let's identify the part
                            if(verifyPart(client,device,slave)):
                                print("growwatt inverter at here",file,baud,par,slave)
                                return
                            pass
                        except Exception as e:
                            pass
                    else:
                        print("nothing on port")
                    client.close()
                    time.sleep(1)
                del client

import json

def find_parameter(device_name, parameter, json_path="modbus_registers.json"):
    with open(json_path) as f:
        data = json.load(f)
    device = data.get(device_name)
    if not device:
        return None
    for block in device.values():
        param_data = block.get("data", {})
        if parameter in param_data:
            result = param_data[parameter].copy()
            result["start_address"] = block.get("start_address")
            result["registers"] = block.get("registers")
            return result
    return None

def getDeviceDetails(part_details : json,client : ModbusSerialClient | ModbusTcpClient,registers_path = "modbus_registers.json",auto_cfg_json = "auto_config_details.json"):
    with open(auto_cfg_json) as cfg_file:
        cfg = json.load(cfg_file)
    
    config_list = cfg["devices"][part_details["device"]]["holding"]["config_list"]
    #print(config_list)
    #print(find_parameter(part["device"],config_list[0]),config_list[0])

    device_details = {}
    #client = ModbusSerialClient(port = part_details["port"],baudrate=part_details["baud"],parity=part_details["parity"],framer=framer.FramerType.RTU)
    #print(client.connect())
    for config in config_list:
        #print("reading config ",config,part_details["device"])
        param_info = find_parameter(part_details["device"], config)
        #print("param_info",param_info)
        if(param_info["registers"] == "hr"):
            regs = client.read_holding_registers(address=param_info["start_address"] + param_info["offset"],count=param_info["size"],slave=part_details["slave_id"])
        else:
            regs = client.read_input_registers(address=param_info["start_address"] + param_info["offset"],count=param_info["size"],slave=part_details["slave_id"])
        if(regs.isError()):
            print("error reading ",config,regs)
            return {}
        value = cltlib.convert_from_registers(regs.registers,getattr(cltlib.DATATYPE,param_info["format"]))
        device_details[config] = value

    #print(device_details)
    return device_details

    



def detectCommDetails(port,device_nums = 1):
    parts_list = []
    devices_found = 0
    with open("auto_config_details.json") as cfg_file:
        cfg = json.load(cfg_file)
    for baud in bauds:
        for par in paritys:
            client = ModbusSerialClient(port = port,baudrate=baud,parity=par,framer=framer.FramerType.RTU)
            for slave in slaves:
                #iterate through all possibilities
                
                if(client.connect()):
                    #connection on port
                    try:
                        #regs = client.read_input_registers(address=1,count=1,slave=slave)
                        #print(regs)
                        for device in cfg["devices"]:
                            #print("checking for ",device)
                            detected = verifyPart(client,device,slave=slave)
                            #print("sleep before next read")
                            time.sleep(0.5)
                            if(detected is True):
                                #print("found device ",device)
                                devices_found += 1
                                part = {
                                    "device": device,
                                    "port": port,
                                    "baud": baud,
                                    "parity": par,
                                    "slave_id": slave
                                }
                                part["device_details"] =  getDeviceDetails(part,client)
                                parts_list.append(part)
                                #print("found device ",device," details ",part["device_details"],device_nums,devices_found)
                                if(devices_found >= device_nums):
                                    return parts_list
                            else:
                                pass
                                #print("could not find device",device," on port",port,baud,par,slave)
                            #since we are able to connect and read , let's identify the part

                        pass
                    except Exception as e:
                        pass
                else:
                    pass
                    #print("nothing on port",port,baud,par,slave)
                client.close()
                time.sleep(1)
            del client
    return parts_list


def verifyPart(client : ModbusSerialClient | ModbusTcpClient, part,slave):
    #read voltages


    with open("auto_config_details.json") as cfg_file:
        cfg = json.load(cfg_file)
    
    with open("modbus_registers.json") as modbus_file:
        maps = json.load(modbus_file)

    device_registers = maps[part]
    input_params = cfg["devices"][part]["input"]["param_list"]
    holding_params = cfg["devices"][part]["holding"]["param_list"]

    for param in input_params:
        param_info = find_parameter(part, param)
        if(param_info is None):
            print("could not find param info",param)
            return False
        address = param_info["start_address"] + param_info["offset"]
        #print(param,address,param_info["registers"])
        try:
            regs = client.read_input_registers(address=address,count=param_info["size"],slave=slave)
            if(regs.isError()):
                #print("error reading ",param,address,regs)
                return False
            if(param_info["m_f"] == "NA"):
                param_info["m_f"] = 1
            if(param_info["s_f"] == "NA"):
                param_info["s_f"] = 0
            #print("param_info",param,param_info)
            data_value = cltlib.convert_from_registers(regs.registers,getattr(cltlib.DATATYPE,param_info["format"])) * param_info["m_f"]
            if(data_value > cfg["param_range"][param][1] or data_value < cfg["param_range"][param][0]):
                #print("value out of range",param,data_value)
                return False

            else:
                #print("read ",param,address,regs.registers)
                pass
        
        except Exception as e:
            print("exception reading ",param,address,e)
            return False
        #print("sleep before next read")
        #time.sleep(0.5)

    return True


def runautoConfig():
    jsonPath = "../../submodules/RpiBackend/app/json_files/installer_cfg.json"

    with open(jsonPath) as installer_cfg:
        installer_inputs = json.load(installer_cfg)
    
    print(installer_inputs)
    device_num = installer_inputs["number_of_devices"]
    provided_details = installer_inputs["device_list"]

    for device in provided_details: 
        device_type = device["device_type"] if "device_type" in device else None
        brand = device["brand"] if "brand" in device else None

        comm_type = device_type["comm_type"]






if __name__ == "__main__":
    runautoConfig()
    """

    part = {
        "device": "growatt",
        "port": "/dev/ttyV0",
        "baud": 9600,
        "parity": 'N',
        "slave_id": 7
    }
    #getDeviceDetails(part)
    #print(find_parameter(part["device"]))
    

    devices = detectCommDetails("/dev/ttyV0",1)
    for device in devices:
        print(device)

    """

    """
    client = ModbusSerialClient(port="/dev/ttyV1",baudrate=9600,parity='N',framer=framer.FramerType.RTU,timeout=1)
    #client = ModbusTcpClient(host="0.0.0.0",port=505,timeout=1)
    print(client.connect())
    print(client.read_input_registers(0,1,slave=1))
    #print(verifyPart(client,"growatt",slave=1))
    with open("auto_config_details.json") as cfg_file:
        cfg = json.load(cfg_file)

    for device in cfg["devices"]:
        print("checking for ",device)
        detected = verifyPart(client,device,slave=1)
        if(detected is not None):
            print("found device ",detected)
        else:
            print("could not find device")

    """




# Example usage:

#print(detectPort(files,"growatt"))