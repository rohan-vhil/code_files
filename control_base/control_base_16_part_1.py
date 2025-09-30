'''Code 1: read_base.py
This file is responsible for all the data acquisition and decoding from your Modbus devices.
It contains all the enums, data models, and the systemDevice class,
which handles the creation of the address maps and the decoding of the data received
from the devices.
The getAgg, getAllData, getLivePower, getFaultData, getAggDG, getAggDGlim, getAggGrid, and getAggLoad functions are also included here as they are all related to reading and processing data.'''


import enum
import math
import logging
import time
from control import error_reporting as err
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

default_Ts = 3
default_Ki = 0.0001
default_Kp = 0.00001
vpp_id: int = 0
site_id: int = 0
mqtt_ip: str = "test.mosquitto.org"
controller_id: str
per_phase_data =['V','I','P','Q','S','En']

agg_data = ['Pf','total_power','total_energy','total_voltage','acfreq','temperature','apparent_power','reactive_power','input_power',"SoC","SoH",'current','import_energy','export_energy','irradiance','ambient_temperature','internal_ambient_temperature','module_temperature','internal_module_temperature']
data_decode = {
    "V" : "voltage",
    "I" : "current",
    "P" : "power",
    "Q" : "Q",
    "S" : "S",
    "En" : "energy"
}
fault_data = ['fault','warning']
component_data = {
    "mppt_voltage": [],
    "mppt_current": [],
    "mppt_power": [],
    "string_voltage": [],
    "string_current": [],
    "string_power": [],
    "cell_voltage": []
}

path_config.path_cfg = path_config.pathConfig()
ENERGY_LOG_PATH = os.path.join(path_config.path_cfg.base_path, 'control', 'total_energy_log.json')

class deviceType(enum.IntEnum):
    solar = 0
    battery = 1
    meter = 2
    EV = 3
    DG=4
    grid = 5
    IO = 6
    load = 7

    @classmethod
    def from_param(cls, obj):
        return int(obj)

class commType(enum.IntEnum):
    modbus_tcp = 0
    modbus_rtu = 1
    can = 2
    api = 3
    ccs2 = 4
    gpio = 5
    none = 6

    @classmethod
    def from_param(cls, obj):
        return int(obj)

class factorType(enum.IntEnum):
    mf_value = 0
    mf_address = 1
    sf_value = 2
    sf_address = 3

    @classmethod
    def from_param(cls, obj):
        return int(obj)


deviceType_l2e = {
    "solar-inverter": deviceType.solar,
    "inverter" : deviceType.solar,
    "battery": deviceType.battery,
    "meter": deviceType.meter,
    "EV": deviceType.EV,
    "DG" : deviceType.DG,
    "grid" : deviceType.grid,
    "IO":deviceType.IO,
    "load" : deviceType.load

}

commType_l2e = {
    "modbus-tcp": commType.modbus_tcp,
    "modbus-rtu": commType.modbus_rtu,
    "CAN": commType.can,
    "API": commType.api,
    "CCS2": commType.ccs2,
    "gpio": commType.gpio
}

deviceType_e2s = {
    deviceType.solar : "inverter",
    deviceType.battery : "battery",
    deviceType.meter : "meter",
    deviceType.EV : "EV",
}

def scaleData(data, scale_factor):
    x = 0

    if type(data) == list:
        for i in range(len(data)):
            x = x + (data[len(data) - i - 1] << i * 16)

    else:
        x = data

    return x * scale_factor

class dataModel:
    valueFromReg: None
    value: float
    batch_start_addr: int
    data: None
    size: int = 0
    decoderFunc: None = None
    encoderFunc: None = None
    factorDecoderFunc: None = None
    factor_type: factorType
    factor_value: float
    has_en:bool=False
    en_block :int=0
    en_offset :int = 0
    data_error: bool = False
    prev_correct_value: float = 0
    block_num: int = 0
    offset: int = 0
    factor_block: int=0
    factor_offset: int=0
    byteorder: str = "BIG"
    wordorder: str = "LITTLE"
    model_present : bool = False
    en_start_addr:int=0
    mode_offset :int = 0
    mode_block : int =0
    mode_start_addr : int = 0
    has_mode : bool = False

    def __init__(self, addr: str="", valuefunc=scaleData, scale_factor=1) -> None:
        self.valueFromReg = valuefunc
        self.addr = addr
        self.value = 0
        self.factor_type = factorType.mf_value
        self.factor_value = 1
        pass

    def getData(self, data) -> None:
        try:
            decoded = BinaryPayloadDecoder.fromRegisters(self.data, wordorder=getattr(Endian, self.wordorder), byteorder=getattr(Endian, self.byteorder))
            if(self.decoderFunc == None):
                return
            else:
                tmp = getattr(decoded, self.decoderFunc)()
            if self.factor_type == factorType.mf_value:
                self.value = tmp * self.factor_value
            elif self.factor_type == factorType.sf_address:
                decoded_factor = BinaryPayloadDecoder.fromRegisters(
                    [data[self.factor_block][self.factor_offset]], wordorder=getattr(Endian, self.wordorder), byteorder=getattr(Endian, self.byteorder)
                )
                self.value = tmp * (10 ** getattr(decoded_factor, self.factorDecoderFunc)())
            else:
                self.value = tmp
        except Exception as e:
            logging.warning(str(e) + str(" block : ") + str(self.block_num) + str(" offset : ") + str(self.offset))

    def decodeFactor(self):
        self.factor_value = -1
        pass

    def getFactors(self, data):
        try:
            if self.factor_type == factorType.sf_address:
                decoded_factor = BinaryPayloadDecoder.fromRegisters(
                    [data[self.factor_block][self.factor_offset]], Endian.BIG
                )
                self.factor_value = decoded_factor.decode_16bit_int()
        except Exception as e:
            logging.warning(str(e) + str(data))

    def encode(self):
        data = self.value
        if self.factor_type == factorType.sf_address:
            decoded = BinaryPayloadBuilder()
            tmp = int(data / 10**self.factor_value)
            getattr(decoded, self.decoderFunc)(tmp)
            payload = decoded.build()
            return payload
        elif self.factor_type == factorType.mf_value:
            decoded = BinaryPayloadBuilder()
            tmp = int(data * self.factor_value)
            getattr(decoded, self.decoderFunc)(tmp)
            payload = decoded.build()
            return payload


class measuredData:
    def __init__(self, phase=1):
        self.phase: int = phase
        self.validated: bool = False
        self.rev_correct_en: float = 0


def getTwosComp(data):
    data = int(data)
    return data if (data < 0x8000) else data - (1 << 16)


class controlData:
    power_pct_stpt: dataModel
    power_stpt: dataModel
    device_state: dataModel
    poweer_lt : dataModel

    def __init__(self) -> None:
        self.power_pct_stpt = dataModel("power stpt", scaleData, 1)
        self.device_state = dataModel("device state", scaleData, 1)
        self.poweer_lt = dataModel("power_limit",scaleData,1)
        pass

class systemDevice:
    device_type: deviceType
    measured_data: measuredData
    control_data: controlData
    comm_type: commType
    comm_details = None
    rated_power = 3800
    storage_capacity = 0
    stptCurve = None
    device_id: int
    addr_map: dict = {}
    a:dict = {}
    err_registers: err.errRegistor
    num_phases :int=1
    phase :str="A"
    connected_to : str = ""
    minimum_limit : int = 0
    ctrl_registers: dict = {}

    def __init__(self, devicetype, commtype, cfg,rated_power=3800,storage_capacity=0) -> None:
        self.device_type = devicetype
        self.comm_type = commtype
        self.rated_power = rated_power
        self.storage_capacity = storage_capacity
        self.measured_data = measuredData()
        self.control_data = controlData()

    def createMapForVar(self, var: dataModel, batch, i, var_name):
        var.model_present = True
        x = self.addr_map["map"][batch]["data"][var_name]
        var.byteorder = self.addr_map["map"][batch]["byteorder"]
        var.wordorder = self.addr_map["map"][batch]["wordorder"]
        var.block_num = i
        var.offset = x["offset"]
        var.size = x["size"]
        var.batch_start_addr = self.addr_map["map"][batch]["start_address"]
        if "s_f" in x and x["s_f"] != "NA":
            if type(x["s_f"]) == str:
                var.factor_type = factorType.sf_address
                j = 0
                for section in self.addr_map["map"]:
                    if x["s_f"] in self.addr_map["map"][section]["data"]:
                        var.factor_block = j
                        var.factor_offset = self.addr_map["map"][section]["data"][x["s_f"]]["offset"]
                        var.factorDecoderFunc = self.addr_map["map"][section]["data"][x["s_f"]]["format"]
                    j = j + 1
        if "m_f" in x and x["m_f"] != "NA":
            if type(x["m_f"]) == float or type(x["m_f"]) == int:
                var.factor_type = factorType.mf_value
                var.factor_value = x["m_f"]
        var.decoderFunc = x["format"]

    def createErrorMap(self, part_num):
        self.err_registers = err.errRegistor(self.addr_map, part_num)

    def createMeasureMap(self,part_num):
        with open('modbus_mappings/mappings.json') as mapfile:
            self.addr_map['map'] = json.load(mapfile)[part_num]

    def createMeasureRegisterMap(self):
        i = 0
        staged_phase_data = {}
        staged_component_data = {key: {} for key in component_data.keys()}

        for batch in self.addr_map["map"]:
            map_data = self.addr_map["map"][batch]["data"]
            for param_name in map_data:
                is_processed = False

                if param_name.startswith('L') and '_' in param_name:
                    try:
                        prefix, type_long = param_name.split('_', 1)
                        phase_idx = int(prefix[1:]) - 1
                        type_short = next((s for s, l in data_decode.items() if l == type_long), None)
                        if type_short and 0 <= phase_idx < self.num_phases:
                            if type_short not in staged_phase_data:
                                staged_phase_data[type_short] = [None] * self.num_phases
                            if staged_phase_data[type_short][phase_idx] is None:
                                model = dataModel()
                                self.createMapForVar(model, batch, i, param_name)
                                staged_phase_data[type_short][phase_idx] = model
                            is_processed = True
                    except (ValueError, KeyError, StopIteration):
                        pass

                if not is_processed:
                    for base_param in component_data.keys():
                        parts = base_param.split('_')
                        prefix = parts[0]
                        suffix = '_'.join(parts[1:])
                        if param_name.startswith(prefix) and param_name.endswith(suffix) and param_name != base_param:
                            try:
                                num_str = param_name[len(prefix):-(len(suffix) + 1)]
                                if num_str.isdigit():
                                    idx = int(num_str) - 1
                                    model = dataModel()
                                    self.createMapForVar(model, batch, i, param_name)
                                    staged_component_data[base_param][idx] = model
                                    is_processed = True
                                    break
                            except (ValueError, IndexError):
                                continue

                if not is_processed:
                    if param_name in agg_data:
                        if not hasattr(self.measured_data, param_name):
                            model = dataModel()
                            self.createMapForVar(model, batch, i, param_name)
                            setattr(self.measured_data, param_name, model)
                    elif param_name in fault_data:
                        if not hasattr(self.measured_data, param_name):
                            model = dataModel()
                            self.createMapForVar(model, batch, i, param_name)
                            setattr(self.measured_data, param_name, model)
            i += 1

        for type_short, models in staged_phase_data.items():
            final_list = [m for m in models if m is not None]
            if final_list:
                setattr(self.measured_data, type_short, final_list)

        for base_param, model_dict in staged_component_data.items():
            if model_dict:
                sorted_models = [model_dict[k] for k in sorted(model_dict.keys())]
                setattr(self.measured_data, base_param, sorted_models)

    def decodeData(self, data_set):
        data = data_set['read']
        control_data = data_set['control']
        if data == [[]]:
            return

        for x in per_phase_data:
            if hasattr(self.measured_data, x):
                for model_instance in getattr(self.measured_data, x):
                    if model_instance:
                        model_instance.data = data[model_instance.block_num][model_instance.offset : model_instance.offset + model_instance.size]
                        model_instance.getData(data)

        for x in agg_data:
            if hasattr(self.measured_data, x):
                model_instance = getattr(self.measured_data, x)
                if model_instance:
                    model_instance.data = data[model_instance.block_num][model_instance.offset : model_instance.offset + model_instance.size]
                    model_instance.getData(data)

        for x in component_data.keys():
            if hasattr(self.measured_data, x):
                for model_instance in getattr(self.measured_data, x):
                    if model_instance:
                        model_instance.data = data[model_instance.block_num][model_instance.offset : model_instance.offset + model_instance.size]
                        model_instance.getData(data)

        for x in fault_data:
            if hasattr(self.measured_data, x):
                model_instance = getattr(self.measured_data, x)
                if model_instance:
                    model_instance.data = data[model_instance.block_num][model_instance.offset : model_instance.offset + model_instance.size]
                    model_instance.getData(data)

    def writeToRegisters(self, data, address):
        if (self.comm_type == commType.modbus_rtu or self.comm_type == commType.modbus_tcp):
            mbus.writeModbusData(self, address, data)


device_list = []
system_operating_details = None


def getAgg(device_type):
    tmp = 0
    agg_power = 0
    agg_capacity = 0
    i = 0
    for device in device_list:
        if device.device_type == device_type:
            if hasattr(device.measured_data, 'total_power'):
                tmp += device.measured_data.total_power.value
            agg_power += device.rated_power
            if device.device_type == deviceType.battery:
                agg_capacity += device.storage_capacity
    if device_type == deviceType.battery:
        return tmp, agg_power, agg_capacity
    else:
        return tmp, agg_power

def getAllData():
    data = {}
    try:
        with open(ENERGY_LOG_PATH, 'r') as f:
            energy_log = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        energy_log = {}

    for device in device_list:
        device_id_str = str(device.device_id)
        data[device_id_str] = {}

        if device.device_type in deviceType_e2s:
            data[device_id_str]["type"] = str(device.num_phases) + "ph_" + deviceType_e2s[device.device_type]

        for param in per_phase_data:
            if hasattr(device.measured_data, param):
                i = 0
                for x in getattr(device.measured_data, param):
                    if x is not None:
                        data[device_id_str]["L" + str(i + 1) + "_" + data_decode[param]] = x.value
                    i += 1

        for param in agg_data:
            if hasattr(device.measured_data, param):
                model = getattr(device.measured_data, param)
                if model is not None:
                    if param == 'total_energy':
                        current_energy = model.value
                        logged_energy = energy_log.get(device_id_str, 0)

                        final_energy = logged_energy
                        if current_energy >= logged_energy and (logged_energy == 0 or (current_energy - logged_energy) <= 100):
                            final_energy = current_energy
                            energy_log[device_id_str] = final_energy

                        data[device_id_str][param] = final_energy
                    else:
                        data[device_id_str][param] = model.value

        for base_param in component_data.keys():
            if hasattr(device.measured_data, base_param):
                models = getattr(device.measured_data, base_param)
                parts = base_param.split('_', 1)
                category = parts[0]
                measurement_type = parts[1]

                if category not in data[device_id_str]:
                    data[device_id_str][category] = {}

                idx = 1
                for model in models:
                    if model is not None and model.model_present:
                        output_name = f"{category}{idx}_{measurement_type}"
                        data[device_id_str][category][output_name] = round(model.value, 2)
                    idx += 1

    with open(ENERGY_LOG_PATH, 'w') as f:
        json.dump(energy_log, f)

    return data

def getLivePower():
    live_power_data = {}
    for device in device_list:
        if hasattr(device.measured_data, 'total_power'):
            model = getattr(device.measured_data, 'total_power')
            if model is not None and model.model_present:
                device_id_str = str(device.device_id)
                live_power_data[device_id_str] = model.value
    return live_power_data

def getFaultData():
    fault_output = {}
    for device in device_list:
        device_faults = {}
        for param in fault_data:
            if hasattr(device.measured_data, param):
                model = getattr(device.measured_data, param)
                if model is not None and model.model_present:
                    device_faults[param] = model.value

        if device_faults:
            fault_output[str(device.device_id)] = device_faults

    return fault_output

def getAggDG():
    global system_operating_details
    if system_operating_details is None:
        return
    system_operating_details.aggDG = 0
    for device in device_list:
        if(device.device_type == deviceType.meter and device.connected_to == deviceType.DG):
             if hasattr(device.measured_data, 'total_power'):
                system_operating_details.aggDG += device.measured_data.total_power.value

def getAggDGlim():
    global system_operating_details
    if system_operating_details is None:
        return
    system_operating_details.dg_lim = 0
    for device in device_list:
        if(device.device_type == deviceType.meter and device.connected_to == deviceType.DG):
            system_operating_details.dg_lim += device.minimum_limit

def getAggGrid():
    global system_operating_details
    if system_operating_details is None:
        return
    system_operating_details.aggGrid = 0
    for device in device_list:
        if(device.device_type == deviceType.meter and device.connected_to == deviceType.grid):
            if hasattr(device.measured_data, 'total_power'):
                system_operating_details.aggGrid += device.measured_data.total_power.value

def getAggLoad():
    global system_operating_details
    if system_operating_details is None:
        return
    system_operating_details.aggLoad = 0
    for device in device_list:
        if(device.device_type == deviceType.meter and device.connected_to == deviceType.load):
            if hasattr(device.measured_data, 'total_power'):
                system_operating_details.aggLoad += device.measured_data.total_power.value