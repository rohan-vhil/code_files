'''I've added the getLivePower function right below the getAllData function as you requested.

This new function specifically extracts the total_power from each device and returns it in a simple dictionary format {device_id: total_power}. No other parts of the code have been changed.'''



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
CONTROL_JSON_PATH = os.path.join(path_config.path_cfg.base_path, 'control', 'control.json')
COST_JSON_PATH = os.path.join(path_config.path_cfg.base_path, 'control', 'cost.json')
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
    none = 12

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

    def __init__(self):
        self.aggDG = 0
        self.system_operating_mode = None

    def controlFuncConstPower(self):
        self.storage_stpt = (100 * (system_operating_details.aggLoad - system_operating_details.aggPV - system_operating_details.ref) / system_operating_details.agg_batt_rated)
        print(system_operating_details.load,system_operating_details.aggPV,system_operating_details.ref)

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
                        # A valid reading must be greater than or equal to the logged value
                        # and the increase must not be abnormally large.
                        if current_energy >= logged_energy and (logged_energy == 0 or (current_energy - logged_energy) <= 100):
                             # Plausible value, use the new reading
                            final_energy = current_energy
                            energy_log[device_id_str] = final_energy
                        # Otherwise, if the reading is lower or jumps too high, we stick with the last good value (`final_energy` remains `logged_energy`)

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
        # Check if the 'total_power' attribute exists on the measured_data object
        if hasattr(device.measured_data, 'total_power'):
            model = getattr(device.measured_data, 'total_power')
            # Ensure the model itself is not None and has been populated
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

def getDeviceType(device_id):
    for device in device_list:
        if(device.device_id == device_id):
            return
    pass

system_operating_details = operatingDetails()