



import enum
import math
import logging
import time
from control import error_reporting as err
from control import control_der as ctrl_der
from modbus_master import modbusmasterapi as mbus
import platform
import sys
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.constants import Endian
import sys
sys.path.insert(0,'../')
import json
import path_config
import datetime
import os
from read_base import (
    deviceType, deviceType_e2s, commType, commType_l2e, device_list, systemDevice,
    system_operating_details, getAgg, getAggDG, getAggDGlim, getAggGrid, getAggLoad
)

path_config.path_cfg = path_config.pathConfig()
CONTROL_JSON_PATH = os.path.join(path_config.path_cfg.base_path, 'control', 'control.json')
COST_JSON_PATH = os.path.join(path_config.path_cfg.base_path, 'control', 'cost.json')

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
    none = 12

    @classmethod
    def from_param(cls, obj):
        return int(obj)


class modeSrc(enum.IntEnum):
    direct_comm = 0
    from_schedule = 1
    no_src = 2

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
    power_pct_stpt: None
    power_stpt: None
    device_state: None
    poweer_lt : None

    def __init__(self) -> None:
        self.power_pct_stpt = None
        self.device_state = None
        self.poweer_lt = None

class systemDevice(systemDevice):
    ctrl_registers : ctrl_der.controlRegistor
    def __init__(self, devicetype, commtype, cfg,rated_power=3800,storage_capacity=0) -> None:
        super().__init__(devicetype, commtype, cfg, rated_power, storage_capacity)
        if devicetype == deviceType.solar:
            self.stptCurve = PVCurveFunc
            system_operating_details.agg_pv_rated += rated_power
        elif devicetype == deviceType.battery:
            self.stptCurve = batteryCurveFunc
            system_operating_details.agg_batt_rated += rated_power
            system_operating_details.battery_storage_capacity += storage_capacity

    def createMapForCtrlVar(self, var, batch, i, var_name):
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
                var.factor_type = factorType.sf_address
                j = 0
                for section in self.ctrl_map["map"]:
                    if x["s_f"] in self.ctrl_map["map"][section]["data"]:
                        var.factor_block = j
                        var.factor_offset = self.ctrl_map["map"][section]["data"][x["s_f"]]["offset"]
                        var.factorDecoderFunc = self.ctrl_map["map"][section]["data"][x["s_f"]]["format"]
                    j = j + 1
        if "m_f" in x and x["m_f"] != "NA":
            if type(x["m_f"]) == float or type(x["m_f"]) == int:
                var.factor_type = factorType.mf_value
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

    def createControlMap(self, part_num):
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
                    self.writeDataToRegisters( mbus.bytes_to_registers(payload),self.control_data.poweer_lt.mode_start_addr + self.control_data.poweer_lt.mode_offset)
                decoded = BinaryPayloadBuilder()
                decoded.add_16bit_uint(True)
                payload=decoded.build()
                self.writeDataToRegisters( mbus.bytes_to_registers(payload),self.control_data.poweer_lt.en_start_addr + self.control_data.poweer_lt.en_offset)
                self.writeDataToRegisters( mbus.bytes_to_registers(self.control_data.poweer_lt.encode()),self.control_data.poweer_lt.batch_start_addr + self.control_data.poweer_lt.offset)
            elif(self.control_data.power_pct_stpt.model_present):
                self.control_data.power_pct_stpt.value = int(eval(msg_json['value'])*100/self.rated_power)
                if self.control_data.power_pct_stpt.value == 0:
                    self.control_data.power_pct_stpt.value = 1
                self.writeDataToRegisters( mbus.bytes_to_registers(self.control_data.power_pct_stpt.encode()),self.control_data.power_pct_stpt.batch_start_addr + self.control_data.power_pct_stpt.offset)
                decoded = BinaryPayloadBuilder()
                decoded.add_16bit_uint(True)
                payload=decoded.build()
                self.writeDataToRegisters( mbus.bytes_to_registers(payload),self.control_data.power_pct_stpt.en_start_addr + self.control_data.power_pct_stpt.en_offset)

    def writeDataToRegisters(self, data, address):
        if (self.comm_type == commType.modbus_rtu or self.comm_type == commType.modbus_tcp):
            mbus.writeModbusData(self, address, data)


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

    def __init__(self):
        self.aggDG = 0
        self.system_operating_mode = None

    def controlFuncConstPower(self):
        self.storage_stpt = (100 * (system_operating_details.aggLoad - system_operating_details.aggPV - system_operating_details.ref) / system_operating_details.agg_batt_rated)

    def controlPVChargeOnly(self):
        self.storage_stpt = (100 * (-system_operating_details.aggPV) / system_operating_details.agg_batt_rated)

    def controlFuncFullBackup(self):
        self.storage_stpt = -100

    def controlFuncFullExport(self):
        self.storage_stpt = 100

    def controlFuncGenLimit(self):
        self.pv_stpt = self.ref

    def controlFuncNone(self):
        self.storage_stpt = 0
        self.pv_stpt = 100

    def time_of_use_func(self):
        with(open(COST_JSON_PATH))as cost_file:
            cost_cfg = json.load(cost_file)
            costs = cost_cfg["cost"]
        N = len(costs)
        avg_cost = sum(costs)/N
        abs_sum = sum([abs(x - avg_cost) for x in costs])
        print("battery_Storage : ",self.battery_storage_capacity)
        alpha = 2*5000 / abs_sum
        now_time = datetime.datetime.now()
        now_minutes = now_time.hour*60 + now_time.minute
        index = int(now_minutes / (24*60/N))
        print("power :",alpha * (costs[index] - avg_cost),"cost : ",costs[index],"avg : ",avg_cost,"alpha : ",alpha)
        self.storage_stpt= alpha * (costs[index] - avg_cost) *100/ 5000

    def dr_based_batt_func(self):
        print(time.time())
        time_diff = system_operating_details.event_end_time - system_operating_details.event_start_time
        storage_capacity = system_operating_details.battery_storage_capacity
        if(time.time() > system_operating_details.event_start_time and time.time() < system_operating_details.event_end_time):
            stpt = min(system_operating_details.battery_storage_capacity  / time_diff,system_operating_details.ref)
        else:
            stpt = -system_operating_details.battery_storage_capacity  / (24 - time_diff/3600)
            pass
        self.storage_stpt = stpt

    def daily_peak_th_base_func(self):
        self.storage_stpt = min(system_operating_details.agg_batt_rated,abs(system_operating_details.ref - system_operating_details.aggGrid)) * (2*(system_operating_details.aggGrid > system_operating_details.ref) - 1)

    def export_limit_func(self):
        self.pv_stpt = max(min(self.aggLoad - self.ref - self.aggBatt,self.agg_pv_rated),0)

    def dg_pv_sync_func(self):
        self.pv_stpt = max(min(self.agg_pv_rated,self.aggDG + self.aggPV - self.dg_lim),0)
        print("DG params : ","PVrated",self.agg_pv_rated,"PV : ",self.aggPV,"DG : ",self.aggDG,"DG_lim",self.dg_lim,"PV stpt",self.pv_stpt)

    def export_lim_export_priority_func(self):
        self.storage_stpt = min(0,self.aggBatt + min(0,self.ref+self.aggGrid))

    def safety_control_func(self):
        if self.safety_control_mode == 0:
            io.ioDevice.write_digital_outputs(0)
        elif self.safety_control_mode == 1:
            io.ioDevice.write_digital_outputs(1, self.io_output_data)

system_operating_details = operatingDetails()

def setParameter(data_json):
    if("mode" in data_json.keys()):
        updateOperatingMode(data_json['mode'])
    if "param" in data_json.keys():
        if(data_json['param'] == "active_power"):
            print(data_json['value'])
            for device in device_list:
                if(device.device_id == eval(data_json['device_id'])):
                    device.encodeWrite(data_json)
            pass
    if "device_state" in data_json.keys():
        address = device.control_data.device_state.batch_start_addr + device.control_data.device_state.offset
        data_to_ctrl:dict = {
            "address": address,
            "format" : device.control_data.device_state.decoderFunc,
            "value" : data_json["device_state"],
            "wo" : device.control_data.device_state.wordorder,
            "bo":device.control_data.device_state.byteorder
        }
        device.writeDataToCtrlRegisters(data_to_ctrl)

def updateOperatingMode(mode_str, ref=0):
    system_operating_details.system_operating_mode = operatingMode_l2e[mode_str]
    if mode_str == "net_zero":
        system_operating_details.ref = 0
        system_operating_details.scs_min = 0
        system_operating_details.scs_max = 1
        system_operating_details.controlFunc = (system_operating_details.controlFuncConstPower)
    if mode_str == "pv_charge_only":
        system_operating_details.ref = 0
        system_operating_details.scs_min = 0.5
        system_operating_details.scs_max = 1
        system_operating_details.controlFunc = (system_operating_details.controlPVChargeOnly)
    if mode_str == "max_export":
        system_operating_details.ref = 0
        system_operating_details.scs_max = 0
        system_operating_details.scs_min = 0
        system_operating_details.controlFunc = (system_operating_details.controlFuncFullExport)
    if mode_str == "power_backup":
        system_operating_details.ref = 0
        system_operating_details.scs_max = 1
        system_operating_details.scs_min = 1
        system_operating_details.controlFunc = (system_operating_details.controlFuncFullBackup)
    if mode_str == "gen_limit":
        system_operating_details.ref = ref
        system_operating_details.scs_max = 1
        system_operating_details.scs_min = 1
        system_operating_details.controlFunc = (system_operating_details.controlFuncGenLimit)
    if mode_str == "none":
        system_operating_details.controlFunc = (system_operating_details.controlFuncNone)
    if mode_str == "safety_control_mode":
        system_operating_details.controlFunc = system_operating_details.safety_control_func
    logging.warning(str(time.time()) + "change mode to: " + mode_str + " ref value : " + str(ref))


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

def SetController(Kp,Ki,Ts):
    system_operating_details.Kp = Kp
    system_operating_details.Ki = Ki
    system_operating_details.Ts = Ts

def curtailStateToStpt():
    for device in device_list:
        if device.stptCurve != None:
            device.control_data.setpt = device.stptCurve()
            data = {"AC Active_powerSetPct": device.control_data.setpt}
            device.writeDataToRegisters(data)

def setModeSrc(src : str):
    if(src == "schedule"):
        system_operating_details.mode_src  = modeSrc.from_schedule
    elif(src == "direct"):
        system_operating_details.mode_src = modeSrc.direct_comm

def processMQTTMessage(message : str):
    print("")
    print("==================")
    print("")
    with(open(CONTROL_JSON_PATH,'w') as control_file):
        control_file.write(message)
    print("====================")
    print("")

def getActiveControlMode():
    if(system_operating_details.aggDG > 0):
        system_operating_details.system_operating_mode = systemOperatingModes.dg_pv_sync
        system_operating_details.controlFunc = system_operating_details.dg_pv_sync_func
        system_operating_details.ref = system_operating_details.dg_lim
    else:
        print("agg_dg somehow got : ",system_operating_details.aggDG)
        try:
            with(open(CONTROL_JSON_PATH) as control_file):
                control_json = json.load(control_file)
                if("mode" in control_json):
                    system_operating_details.system_operating_mode = getattr(systemOperatingModes,control_json["mode"])
                    system_operating_details.controlFunc = getattr(system_operating_details,control_json["mode"] + "_func")
                    system_operating_details.ref = control_json["op_details"]["ref"]
                else:
                    system_operating_details.controlFunc = None
        except (json.JSONDecodeError, FileNotFoundError):
            system_operating_details.controlFunc = None
            print("Control JSON not found or invalid.")
    print("func is",system_operating_details.controlFunc)


def runSysControlLoop():
    system_operating_details.aggPV,system_operating_details.agg_pv_rated = getAgg(deviceType.solar)
    system_operating_details.aggBatt,system_operating_details.agg_batt_rated, system_operating_details.battery_storage_capacity = getAgg(deviceType.battery)
    system_operating_details.aggEV, aggEV = getAgg(deviceType.EV)
    getAggLoad()
    getAggDGlim()
    getAggDG()
    getActiveControlMode()
    if system_operating_details.controlFunc != None:
        system_operating_details.controlFunc()
        for device in device_list:
            if device.device_type == deviceType.battery:
                print("====battery device =====")
                power = system_operating_details.storage_stpt
                data_msg = {"param" : "active_power","value":str(power)}
                print(data_msg,system_operating_details.storage_stpt)
                device.encodeWrite(data_msg)
            if device.device_type == deviceType.solar:
                device.control_data.power_pct_stpt.value = system_operating_details.pv_stpt
                data_msg = {"param" : "active_power","value":str(device.control_data.power_pct_stpt.value)}
                print(data_msg)
                device.encodeWrite(data_msg)