'''
Refactor Control Code into Modules
https://gemini.google.com/share/755d04d35076

control_logic.py
This file contains the control classes, writing logic, and control loops.
It extends the systemDevice class from the first file with control-specific methods.'''


import time
import datetime
import logging
import json
import os
import enum
import sys
sys.path.insert(0,'../')
import path_config
from pymodbus.payload import BinaryPayloadBuilder
import data_acquisition as daq
from control import error_reporting as err
from control import control_der as ctrl_der
from modbus_master import modbusmasterapi as mbus

default_Ts = 3
default_Ki = 0.0001
default_Kp = 0.00001
vpp_id: int = 0
site_id: int = 0
mqtt_ip: str = "test.mosquitto.org"

CONTROL_JSON_PATH = os.path.join(path_config.path_cfg.base_path, 'control', 'control.json')
COST_JSON_PATH = os.path.join(path_config.path_cfg.base_path, 'control', 'cost.json')

class modeSrc(enum.IntEnum):
    direct_comm = 0
    from_schedule = 1
    no_src = 2

    @classmethod
    def from_param(cls, obj):
        return int(obj)

class gridState(enum.IntEnum):
    on =0,
    off =1

    @classmethod
    def from_param(cls, obj):
        return int(obj)

class systemOperatingModes(enum.IntEnum):
    const_power = 0
    pv_charge_only_mode = 1
    full_backup = 2
    max_export = 3
    max_import = 4
    full_backup_with_zn = 5
    gen_limit = 6
    dr_command = 7
    daily_peak_th_base = 8
    daily_peak_time_base = 9
    export_limit = 10,
    dg_pv_sync = 11,
    schedule = 10
    time_of_use = 11
    backup = 12
    none = 13

    @classmethod
    def from_param(cls, obj):
        return int(obj)

operatingMode_l2e = {
    "pv_charge_only": systemOperatingModes.pv_charge_only_mode,
    "net_zero": systemOperatingModes.const_power,
    "power_backup": systemOperatingModes.full_backup,
    "max_export": systemOperatingModes.max_export,
    "gen_limit": systemOperatingModes.gen_limit,
    "none" : systemOperatingModes.none
}

class controlData:
    power_pct_stpt: daq.dataModel
    power_stpt: daq.dataModel
    device_state: daq.dataModel
    poweer_lt : daq.dataModel

    def __init__(self) -> None:
        self.power_pct_stpt = daq.dataModel("power stpt", daq.scaleData, 1)
        self.device_state = daq.dataModel("device state", daq.scaleData, 1)
        self.poweer_lt = daq.dataModel("power_limit",daq.scaleData,1)
        pass

class operatingDetails:
    system_operating_mode = None
    controlFunc = None
    system_curtail_state = 0
    full_pv_curtail = 2
    agg_pv_rated = 0
    agg_batt_rated = 0
    battery_storage_capacity = 0
    aggDG = 0
    dg_lim = 0
    ref = 0
    scs_min = 0
    scs_max = 1
    aggPV = 0
    aggBatt = 0
    aggGrid = 0
    aggLoad = 0
    aggEV = 0
    load = 0
    Ki = 0
    Kp = 0
    Ts = 0
    err = 0
    storage_stpt = 0
    pv_stpt = 100
    safety_control_mode = 0
    io_output_data = None
    mode_src : modeSrc = modeSrc.no_src
    event_start_time : int=0
    event_end_time : int=0
    event_date : str=""
    swtch_curtail_percent = 0
    battery_energy = 0
    grid_state = gridState.on
    storage_min = -100
    storage_max = 100
    solar_max = 100
    limit_export : bool
    live_data : bool = False
    live_data_timer : int = 0

    def __init__(self):
        self.aggDG = 0
        self.system_operating_mode = None
        self.grid_state = gridState.on
        self.limit_export = False
        self.live_data = False
        self.live_data_timer = 0

system_operating_details = operatingDetails()

def batteryCurveFunc():
    if system_operating_details.system_curtail_state < 1:
        return 100 - 200 * system_operating_details.system_curtail_state
    else:
        return -100

def PVCurveFunc():
    if system_operating_details.system_curtail_state < 1:
        return 100
    else:
        return (100 * (system_operating_details.full_pv_curtail - system_operating_details.system_curtail_state) / (system_operating_details.full_pv_curtail - 1))

def createMapForCtrlVar(self, var: daq.dataModel, batch, i, var_name):
    var.model_present = True
    x = self.ctrl_map["map"][batch]["data"][var_name]
    var.byteorder = self.ctrl_map["map"][batch]["byteorder"]
    var.wordorder = self.ctrl_map["map"][batch]["wordorder"]
    var.block_num = i
    var.offset = x["offset"]
    var.size = x["size"]
    var.batch_start_addr = self.ctrl_map["map"][batch]["start_address"]
    if "s_f" in x and x["s_f"] != "NA":
        if type(x["s_f"]) == str:
            var.factor_type = daq.factorType.sf_address
            j = 0
            for section in self.ctrl_map["map"]:
                if x["s_f"] in self.ctrl_map["map"][section]["data"]:
                    var.factor_block = j
                    var.factor_offset = self.ctrl_map["map"][section]["data"][x["s_f"]]["offset"]
                    var.factorDecoderFunc = self.ctrl_map["map"][section]["data"][x["s_f"]]["format"]
                j = j + 1
    if "m_f" in x and x["m_f"] != "NA":
        if type(x["m_f"]) == float or type(x["m_f"]) == int:
            var.factor_type = daq.factorType.mf_value
            var.factor_value = x["m_f"]
    if("switch_register" in x):
        if(x["switch_register"] != "NA" and x["switch_register"] != ""):
            var.en_offset = self.ctrl_map["map"][batch]["data"][x["switch_register"]]["offset"]
            var.en_block = i
            var.en_start_addr = self.ctrl_map["map"][batch]["start_address"]
    if("mode_reg") in x:
        if(x["mode_reg"] != "" and x["mode_reg"] != "NA"):
            var.mode_offset = self.ctrl_map["map"][batch]["data"][x["mode_reg"]]["offset"]
            var.mode_start_addr = self.ctrl_map["map"][batch]["start_address"]
            var.mode_block = i
            var.has_mode = True
    var.decoderFunc = x["format"]

def createErrorMap(self, part_num):
    self.err_registers = err.errRegistor(self.addr_map, part_num)

def createControlMap(self, part_num):
    self.ctrl_map = {}
    with open('modbus_mappings/control_registers.json') as mapfile:
        self.ctrl_map['map'] = json.load(mapfile)[part_num]

def createControlRegisterMap(self):
    i=0
    for batch in self.ctrl_map['map']:
        if('power_limit' in self.ctrl_map["map"][batch]["data"]):
            self.createMapForCtrlVar(self.control_data.poweer_lt,batch,i,"power_limit")
        if('power_limit_pct' in self.ctrl_map["map"][batch]["data"]):
            self.createMapForCtrlVar(self.control_data.power_pct_stpt,batch,i,"power_limit_pct")
        i+=1

def encodeWrite(self, msg_json:dict):
    if msg_json["param"] == "active_power":
        if(self.control_data.poweer_lt.model_present):
            self.control_data.poweer_lt.value = eval(msg_json['value'])
            if(self.control_data.poweer_lt.has_mode):
                decoded = BinaryPayloadBuilder()
                decoded.add_16bit_uint(1)
                payload=decoded.build()
                self.writeToRegisters( mbus.bytes_to_registers(payload),self.control_data.poweer_lt.mode_start_addr + self.control_data.poweer_lt.mode_offset)
            decoded = BinaryPayloadBuilder()
            decoded.add_16bit_uint(True)
            payload=decoded.build()
            self.writeToRegisters( mbus.bytes_to_registers(payload),self.control_data.poweer_lt.en_start_addr + self.control_data.poweer_lt.en_offset)
            self.writeToRegisters( mbus.bytes_to_registers(self.control_data.poweer_lt.encode()),self.control_data.poweer_lt.batch_start_addr + self.control_data.poweer_lt.offset)
        elif(self.control_data.power_pct_stpt.model_present):
            self.control_data.power_pct_stpt.value = int(eval(msg_json['value'])*100/self.rated_power)
            if self.control_data.power_pct_stpt.value == 0:
                self.control_data.power_pct_stpt.value = 1
            self.writeToRegisters( mbus.bytes_to_registers(self.control_data.power_pct_stpt.encode()),self.control_data.power_pct_stpt.batch_start_addr + self.control_data.power_pct_stpt.offset)
            decoded = BinaryPayloadBuilder()
            decoded.add_16bit_uint(True)
            payload=decoded.build()
            self.writeToRegisters( mbus.bytes_to_registers(payload),self.control_data.power_pct_stpt.en_start_addr + self.control_data.power_pct_stpt.en_offset)

def writeToRegisters(self, data, address):
    if (self.comm_type == daq.commType.modbus_rtu or self.comm_type == daq.commType.modbus_tcp):
        mbus.writeModbusData(self, address, data)

daq.systemDevice.createMapForCtrlVar = createMapForCtrlVar
daq.systemDevice.createErrorMap = createErrorMap
daq.systemDevice.createControlMap = createControlMap
daq.systemDevice.createControlRegisterMap = createControlRegisterMap
daq.systemDevice.encodeWrite = encodeWrite
daq.systemDevice.writeToRegisters = writeToRegisters

def initialize_device_control():
    for device in daq.device_list:
        device.control_data = controlData()
        if device.device_type == daq.deviceType.solar:
            device.stptCurve = PVCurveFunc
            system_operating_details.agg_pv_rated += device.rated_power
        elif device.device_type == daq.deviceType.battery:
            device.stptCurve = batteryCurveFunc
            system_operating_details.agg_batt_rated += device.rated_power
            system_operating_details.battery_storage_capacity += device.storage_capacity

def controlFuncConstPower():
    system_operating_details.storage_stpt = max(min((
        100
        * (
            system_operating_details.aggLoad
            - system_operating_details.aggPV
            - system_operating_details.ref
        )
        / system_operating_details.agg_batt_rated
    ),system_operating_details.storage_max),system_operating_details.storage_min)
    print("in cosnt power",system_operating_details.load,system_operating_details.aggPV,system_operating_details.ref,system_operating_details.storage_max,system_operating_details.storage_min)

def controlPVChargeOnly():
    system_operating_details.storage_stpt = (100 * (-system_operating_details.aggPV) / system_operating_details.agg_batt_rated)

def controlFuncFullBackup():
    if(system_operating_details.grid_state == gridState.on):
        system_operating_details.storage_stpt = -100

def controlFuncFullExport():
    system_operating_details.storage_stpt = 100

def controlFuncGenLimit():
    system_operating_details.pv_stpt = system_operating_details.ref

def controlFuncNone():
    system_operating_details.storage_stpt = 0
    system_operating_details.pv_stpt = 100

def time_of_use_func():
    with(open(COST_JSON_PATH))as cost_file:
        cost_cfg = json.load(cost_file)
        costs = cost_cfg["cost"]
    N = len(costs)
    avg_cost = sum(costs)/N
    abs_sum = sum([abs(x - avg_cost) for x in costs])
    print("battery_Storage : ",system_operating_details.battery_storage_capacity)
    alpha = system_operating_details.ref*system_operating_details.battery_storage_capacity / abs_sum
    now_time = datetime.datetime.now()
    now_minutes = now_time.hour*60 + now_time.minute
    index = int(now_minutes / (24*60/N))
    print("power :",alpha * (costs[index] - avg_cost),"cost : ",costs[index],"avg : ",avg_cost,"alpha : ",alpha)
    system_operating_details.storage_stpt= alpha * (costs[index] - avg_cost) *100/ system_operating_details.battery_storage_capacity

def dr_based_batt_func():
    print(time.time())
    time_diff = system_operating_details.event_end_time - system_operating_details.event_start_time
    storage_capacity = system_operating_details.battery_storage_capacity
    if(time.time() > system_operating_details.event_start_time and time.time() < system_operating_details.event_end_time):
        stpt = min(system_operating_details.battery_storage_capacity  / time_diff,system_operating_details.ref)
    else:
        stpt = -system_operating_details.battery_storage_capacity  / (24 - time_diff/3600)
        pass
    system_operating_details.storage_stpt = stpt

def daily_peak_th_base_func():
    system_operating_details.storage_stpt = min(system_operating_details.agg_batt_rated,abs(system_operating_details.ref - system_operating_details.aggGrid)) * (2*(system_operating_details.aggGrid > system_operating_details.ref) - 1)

def export_limit_func():
    grid_power = system_operating_details.aggLoad - system_operating_details.aggBatt - system_operating_details.aggPV
    controlFuncConstPower()
    if(system_operating_details.storage_stpt <= -100 and system_operating_details.limit_export):
        system_operating_details.pv_stpt = max(min(system_operating_details.aggLoad - system_operating_details.ref - system_operating_details.storage_stpt*system_operating_details.agg_batt_rated/100,system_operating_details.agg_pv_rated),0)*100/system_operating_details.agg_pv_rated
        system_operating_details.pv_stpt = min(system_operating_details.solar_max,system_operating_details.pv_stpt)
    else:
        system_operating_details.pv_stpt = 100    

def dg_pv_sync_func():
    tmp = system_operating_details.aggLoad - system_operating_details.dg_lim
    if(system_operating_details.agg_batt_rated > 0):
        system_operating_details.storage_stpt = max(min(tmp - system_operating_details.aggPV,system_operating_details.agg_batt_rated),-system_operating_details.agg_batt_rated)*100/system_operating_details.agg_batt_rated
        if(system_operating_details.storage_stpt <= -100):
            print("tmp : ",tmp,system_operating_details.aggLoad)
            system_operating_details.pv_stpt = min(tmp - system_operating_details.storage_stpt*system_operating_details.agg_batt_rated/100,system_operating_details.agg_pv_rated)*100/system_operating_details.agg_pv_rated
    else:
        system_operating_details.pv_stpt = max(min(system_operating_details.agg_pv_rated,system_operating_details.aggDG + system_operating_details.aggPV - system_operating_details.dg_lim),0)*100/system_operating_details.agg_pv_rated 

def export_lim_export_priority_func():
    system_operating_details.storage_stpt = min(0,system_operating_details.aggBatt + min(0,system_operating_details.ref+system_operating_details.aggGrid))

def safety_control_func():
    if system_operating_details.safety_control_mode == 0:
        pass
    elif system_operating_details.safety_control_mode == 1:
        pass

def setParameter(data_json):
    if("mode" in data_json.keys()):
        updateOperatingMode(data_json['mode'])
    if "param" in data_json.keys():
        if(data_json['param'] == "active_power"):
            print(data_json['value'])
            for device in daq.device_list:
                if(device.device_id == eval(data_json['device_id'])):
                    device.encodeWrite(data_json)
            pass
    if "device_state" in data_json.keys():
        pass

def updateOperatingMode(mode_str, ref=0):
    system_operating_details.system_operating_mode = operatingMode_l2e[mode_str]
    if mode_str == "net_zero":
        system_operating_details.ref = 0
        system_operating_details.scs_min = 0
        system_operating_details.scs_max = 1
        system_operating_details.controlFunc = controlFuncConstPower
    if mode_str == "pv_charge_only":
        system_operating_details.ref = 0
        system_operating_details.scs_min = 0.5
        system_operating_details.scs_max = 1
        system_operating_details.controlFunc = controlPVChargeOnly
    if mode_str == "max_export":
        system_operating_details.ref = 0
        system_operating_details.scs_max = 0
        system_operating_details.scs_min = 0
        system_operating_details.controlFunc = controlFuncFullExport
    if mode_str == "power_backup":
        system_operating_details.ref = 0
        system_operating_details.scs_max = 1
        system_operating_details.scs_min = 1
        system_operating_details.controlFunc = controlFuncFullBackup
    if mode_str == "gen_limit":
        system_operating_details.ref = ref
        system_operating_details.scs_max = 1
        system_operating_details.scs_min = 1
        system_operating_details.controlFunc = controlFuncGenLimit
    if mode_str == "none":
        system_operating_details.controlFunc = controlFuncNone
    if mode_str == "safety_control_mode":
        system_operating_details.controlFunc = safety_control_func
    logging.warning(str(time.time()) + "change mode to: " + mode_str + " ref value : " + str(ref))

def SetController(Kp,Ki,Ts):
    system_operating_details.Kp = Kp
    system_operating_details.Ki = Ki
    system_operating_details.Ts = Ts

def curtailStateToStpt():
    for device in daq.device_list:
        if device.stptCurve != None:
            device.control_data.setpt = device.stptCurve()
            data = {"AC Active_powerSetPct": device.control_data.setpt}
            device.writeToRegisters(data)

def processMQTTMessage(message : str):
    print("")
    print("==================")
    print("")
    with(open(CONTROL_JSON_PATH,'w') as control_file):
        control_file.write(message)
    print("====================")
    print("")

def startLiveData():
    system_operating_details.live_data = True
    system_operating_details.live_data_timer = 60
    print("live data started")

def stopLiveData():
    system_operating_details.live_data = False
    system_operating_details.live_data_timer = 0
    print("live data stopped")

def getActiveControlMode():
    if(system_operating_details.aggDG > 0):
        system_operating_details.system_operating_mode = systemOperatingModes.dg_pv_sync
        system_operating_details.controlFunc = dg_pv_sync_func
        system_operating_details.ref = system_operating_details.dg_lim
    else:
        print("agg_dg somehow got : ",system_operating_details.aggDG)
        try:
            with(open(CONTROL_JSON_PATH)) as control_file:
                control_json = json.load(control_file)
                if("mode" in control_json):
                    system_operating_details.system_operating_mode = getattr(systemOperatingModes,control_json["mode"])
                    system_operating_details.controlFunc = getattr(sys.modules[__name__],control_json["mode"] + "_func")
                    if "ref" in control_json["op_details"]:
                        system_operating_details.ref = control_json["op_details"]["ref"]
                    else:
                        system_operating_details.ref = 0  

                    if "Limit_export" in control_json["op_details"]:
                        system_operating_details.limit_export = control_json["op_details"]["Limit_export"]
                    else:
                        system_operating_details.limit_export = False
                    system_operating_details.storage_min = -100
                    if "batt_to_load" in  control_json["op_details"]:
                        
                        system_operating_details.storage_max = control_json["op_details"]["batt_to_load"] * 100

                    else:
                        system_operating_details.storage_max = 0

                    if "storage_min" in control_json["op_details"]:
                        system_operating_details.storage_min = control_json["op_details"]["storage_min"]
                    else:
                         system_operating_details.storage_min = -100

                    if "storage_max" in control_json["op_details"]:
                        system_operating_details.storage_max = control_json["op_details"]["storage_max"]
                    else:
                        system_operating_details.storage_max = 100

                    if "solar_max" in control_json["op_details"]:
                        system_operating_details.solar_max = control_json["op_details"]["solar_max"]
                    else:
                        system_operating_details.solar_max  = 100  
                else:
                    system_operating_details.controlFunc = None
                    print("mode from json : ",system_operating_details.controlFunc)

        except (json.JSONDecodeError, FileNotFoundError):
            system_operating_details.controlFunc = None
            print("Control JSON not found or invalid.")
    print("func is",system_operating_details.controlFunc)

def getAggDG():
    system_operating_details.aggDG = 0
    for device in daq.device_list:
        if(device.device_type == daq.deviceType.meter and device.connected_to == daq.deviceType.DG):
             if hasattr(device.measured_data, 'total_power'):
                system_operating_details.aggDG += device.measured_data.total_power.value

def getAggDGlim():
    system_operating_details.dg_lim = 0
    for device in daq.device_list:
        if(device.device_type == daq.deviceType.meter and device.connected_to == daq.deviceType.DG):
            print(device.measured_data.total_power.value,device)
            system_operating_details.dg_lim += device.minimum_limit

def getAggGrid():
    system_operating_details.aggGrid = 0
    for device in daq.device_list:
        if(device.device_type == daq.deviceType.meter and device.connected_to == daq.deviceType.grid):
            if hasattr(device.measured_data, 'total_power'):
                system_operating_details.aggGrid += device.measured_data.total_power.value

def getAggLoad():
    system_operating_details.aggLoad = 0
    for device in daq.device_list:
        if(device.device_type == daq.deviceType.meter and device.connected_to == daq.deviceType.load):
            if hasattr(device.measured_data, 'total_power'):
                system_operating_details.aggLoad += device.measured_data.total_power.value
        elif(device.device_type == daq.deviceType.load and device.connected_to == daq.deviceType.grid):
            if hasattr(device.measured_data, 'total_power'):
                system_operating_details.aggLoad += device.measured_data.total_power.value

def runSysControlLoop():
    initialize_device_control()
    while True:
        system_operating_details.aggPV,system_operating_details.agg_pv_rated = daq.getAgg(daq.deviceType.solar)
        system_operating_details.aggBatt,system_operating_details.agg_batt_rated, system_operating_details.battery_storage_capacity = daq.getAgg(daq.deviceType.battery)
        system_operating_details.aggEV, aggEV = daq.getAgg(daq.deviceType.EV)
        getAggLoad()
        getAggDGlim()
        getAggDG()
        getAggGrid()
        getActiveControlMode()
        if system_operating_details.controlFunc != None:
            system_operating_details.controlFunc()
            for device in daq.device_list:
                if device.device_type == daq.deviceType.battery:
                    print("====battery device =====")
                    power = system_operating_details.storage_stpt
                    data_msg = {"param" : "active_power","value":str(power)}
                    print(data_msg,system_operating_details.storage_stpt)
                    device.encodeWrite(data_msg)
                if device.device_type == daq.deviceType.solar:
                    device.control_data.power_pct_stpt.value = system_operating_details.pv_stpt
                    data_msg = {"param" : "active_power","value":str(device.control_data.power_pct_stpt.value)}
                    print(data_msg)
                    device.encodeWrite(data_msg)
        time.sleep(1)