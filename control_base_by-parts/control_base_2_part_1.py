'''
Refactor Control Code into Modules
https://gemini.google.com/share/9626b782e5b0

Here are the two separate files based on your requirements.
File 1: data_acquisition.py
This file contains all data structures, Modbus mapping, device definitions, and aggregation functions.'''


import enum
import logging
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
import time
import os
from control import error_reporting as err
from control import control_der as ctrl_der
from modbus_master import modbusmasterapi as mbus

default_Ts = 3
default_Ki = 0.0001
default_Kp = 0.00001
vpp_id: int = 0
site_id: int = 0
mqtt_ip: str = "test.mosquitto.org"
controller_id: str
per_phase_data =['V','I','P','Q','S','En']

agg_data = ['Pf','total_power','total_energy','today_energy','total_voltage','acfreq','temperature','apparent_power','reactive_power','input_power',"SoC","SoH",'current','import_energy','export_energy','irradiance','ambient_temperature','internal_ambient_temperature','module_temperature','internal_module_temperature','wind_direction','wind_speed','humidity','solar_radition','rain_gauge','global_horizontal_irradiance','global_tilt_irradiance','maximum_charging_current','maximum_discharging_current','available_charging_capacity','available_discharging_capacity','maximum_cell_voltage','cell_number_with_maximum_voltage','minimum_cell_voltage','cell_number_with_minimum_voltage','maximum_cell_temperature','cell_number_with_maximum_temperature','minimum_cell_temperature','cell_number_with_minimum_temperature']
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

operatingMode_l2e = {
    "pv_charge_only": systemOperatingModes.pv_charge_only_mode,
    "net_zero": systemOperatingModes.const_power,
    "power_backup": systemOperatingModes.full_backup,
    "max_export": systemOperatingModes.max_export,
    "gen_limit": systemOperatingModes.gen_limit,
    "none" : systemOperatingModes.none
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

def getTwosComp(data):
    data = int(data)
    return data if (data < 0x8000) else data - (1 << 16)

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
    addition_factor: float = 0
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
        self.addition_factor = 0
        pass

    def getData(self, data) -> None:
        try:
            decoded = BinaryPayloadDecoder.fromRegisters(self.data, wordorder=getattr(Endian, self.wordorder), byteorder=getattr(Endian, self.byteorder))
            if(self.decoderFunc == None):
                return
            else:
                tmp = getattr(decoded, self.decoderFunc)()
            if self.factor_type == factorType.mf_value:
                self.value = (tmp * self.factor_value) + self.addition_factor
            elif self.factor_type == factorType.sf_address:
                decoded_factor = BinaryPayloadDecoder.fromRegisters(
                    [data[self.factor_block][self.factor_offset]], wordorder=getattr(Endian, self.wordorder), byteorder=getattr(Endian, self.byteorder)
                )
                self.value = (tmp * (10 ** getattr(decoded_factor, self.factorDecoderFunc)())) + self.addition_factor
            else:
                self.value = tmp + self.addition_factor
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
        data = self.value - self.addition_factor
        if self.factor_type == factorType.sf_address:
            decoded = BinaryPayloadBuilder()
            tmp = int(data / 10**self.factor_value)
            getattr(decoded, self.decoderFunc)(tmp)
            payload = decoded.build()
            return payload
        elif self.factor_type == factorType.mf_value:
            decoded = BinaryPayloadBuilder()
            tmp = int(data / self.factor_value)
            getattr(decoded, self.decoderFunc)(tmp)
            payload = decoded.build()
            return payload

class measuredData:
    def __init__(self, phase=1):
        self.phase: int = phase
        self.validated: bool = False
        self.rev_correct_en: float = 0

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
    ctrl_registers : ctrl_der.controlRegistor
    num_phases :int=1
    phase :str="A"
    connected_to : str = ""
    minimum_limit : int = 0

    def __init__(self, devicetype, commtype, cfg,rated_power=3800,storage_capacity=0) -> None:
        self.device_type = devicetype
        self.comm_type = commtype
        self.rated_power = rated_power
        self.storage_capacity = storage_capacity
        if devicetype == deviceType.solar:
            self.stptCurve = PVCurveFunc
            system_operating_details.agg_pv_rated += rated_power
        elif devicetype == deviceType.battery:
            self.stptCurve = batteryCurveFunc
            system_operating_details.agg_batt_rated += rated_power
            system_operating_details.battery_storage_capacity += storage_capacity
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
        if "a_f" in x and x["a_f"] != "NA":
            if type(x["a_f"]) == float or type(x["a_f"]) == int:
                var.addition_factor = x["a_f"]
        var.decoderFunc = x["format"]

    def createMapForCtrlVar(self, var: dataModel, batch, i, var_name):
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

    def createErrorMap(self, part_num):
        self.err_registers = err.errRegistor(self.addr_map, part_num)

    def createControlMap(self, part_num):
        with open('modbus_mappings/control_registers.json') as mapfile:
            self.ctrl_map['map'] = json.load(mapfile)[part_num]
    
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
                        if prefix[1:].isdigit():
                            phase_idx = int(prefix[1:]) - 1
                            type_short = next((s for s, l in data_decode.items() if l == type_long), None)
                            if type_short:
                                if type_short not in staged_phase_data:
                                    staged_phase_data[type_short] = {}
                                
                                if phase_idx not in staged_phase_data[type_short]:
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
            
        for type_short, idx_map in staged_phase_data.items():
            if idx_map:
                max_idx = max(idx_map.keys())
                full_list = [None] * (max_idx + 1)
                for idx, model in idx_map.items():
                    full_list[idx] = model
                setattr(self.measured_data, type_short, full_list)
        
        for base_param, idx_map in staged_component_data.items():
            if idx_map:
                max_idx = max(idx_map.keys())
                full_list = [None] * (max_idx + 1)
                for idx, model in idx_map.items():
                    full_list[idx] = model
                setattr(self.measured_data, base_param, full_list)

    def createControlRegisterMap(self):
        i=0
        for batch in self.ctrl_map['map']:
            if('power_limit' in self.ctrl_map["map"][batch]["data"]):
                self.createMapForCtrlVar(self.control_data.poweer_lt,batch,i,"power_limit")
            if('power_limit_pct' in self.ctrl_map["map"][batch]["data"]):
                self.createMapForCtrlVar(self.control_data.power_pct_stpt,batch,i,"power_limit_pct")
            i+=1
        pass

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

        if(self.control_data.poweer_lt.model_present):
            self.control_data.poweer_lt.getFactors(control_data)
        if(self.control_data.power_pct_stpt.model_present):
            self.control_data.power_pct_stpt.getFactors(control_data)

    def writeToRegisters(self, data, address):
        if (self.comm_type == commType.modbus_rtu or self.comm_type == commType.modbus_tcp):
            mbus.writeModbusData(self, address, data)

device_list = []

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

def setModeSrc(src : str):
    if(src == "schedule"):
        system_operating_details.mode_src  = modeSrc.from_schedule
    elif(src == "direct"):
        system_operating_details.mode_src = modeSrc.direct_comm

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
                    if param == 'total_power':
                        value = model.value
                        if 'HT' in device_id_str and value < 0:
                            value *= -1
                        data[device_id_str][param] = value
                    elif param == 'total_energy':
                        current_energy = model.value
                        logged_energy = energy_log.get(device_id_str, 0)
                        final_energy = logged_energy
                        if current_energy >= logged_energy and (logged_energy == 0 or (current_energy - logged_energy) <= 1000):
                            final_energy = current_energy
                            energy_log[device_id_str] = final_energy
                        if final_energy > 0:
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
    system_operating_details.aggDG = 0
    for device in device_list:
        if(device.device_type == deviceType.meter and device.connected_to == deviceType.DG):
             if hasattr(device.measured_data, 'total_power'):
                system_operating_details.aggDG += device.measured_data.total_power.value

def getAggDGlim():
    system_operating_details.dg_lim = 0
    for device in device_list:
        if(device.device_type == deviceType.meter and device.connected_to == deviceType.DG):
            print(device.measured_data.total_power.value,device)
            system_operating_details.dg_lim += device.minimum_limit

def getAggGrid():
    system_operating_details.aggGrid = 0
    for device in device_list:
        if(device.device_type == deviceType.meter and device.connected_to == deviceType.grid):
            if hasattr(device.measured_data, 'total_power'):
                system_operating_details.aggGrid += device.measured_data.total_power.value

def getAggLoad():
    system_operating_details.aggLoad = 0
    for device in device_list:
        if(device.device_type == deviceType.meter and device.connected_to == deviceType.load):
            if hasattr(device.measured_data, 'total_power'):
                system_operating_details.aggLoad += device.measured_data.total_power.value
        elif(device.device_type == deviceType.load and device.connected_to == deviceType.grid):
            if hasattr(device.measured_data, 'total_power'):
                system_operating_details.aggLoad += device.measured_data.total_power.value

def getDeviceType(device_id):
    for device in device_list:
        if(device.device_id == device_id):
            return
    pass

def read_data_thread():
    while True:
        getAgg(deviceType.solar)
        getAgg(deviceType.battery)
        getAgg(deviceType.EV)
        getAggLoad()
        getAggDGlim()
        getAggDG()
        getAggGrid()
        time.sleep(0.5)