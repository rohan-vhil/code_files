import time
import datetime
import logging
import json
import os
import sys
sys.path.insert(0,'../')
import path_config
import data_acquisition as daq

CONTROL_JSON_PATH = os.path.join(path_config.path_cfg.base_path, 'control', 'control.json')
COST_JSON_PATH = os.path.join(path_config.path_cfg.base_path, 'control', 'cost.json')

def controlFuncConstPower():
    daq.system_operating_details.storage_stpt = max(min((
        100
        * (
            daq.system_operating_details.aggLoad
            - daq.system_operating_details.aggPV
            - daq.system_operating_details.ref
        )
        / daq.system_operating_details.agg_batt_rated
    ),daq.system_operating_details.storage_max),daq.system_operating_details.storage_min)

    print("in cosnt power",daq.system_operating_details.load,daq.system_operating_details.aggPV,daq.system_operating_details.ref,daq.system_operating_details.storage_max,daq.system_operating_details.storage_min)

def controlPVChargeOnly():
    daq.system_operating_details.storage_stpt = (100 * (-daq.system_operating_details.aggPV) / daq.system_operating_details.agg_batt_rated)

def controlFuncFullBackup():
    if(daq.system_operating_details.grid_state == gridState.on):
        daq.system_operating_details.storage_stpt = -100

def controlFuncFullExport():
    daq.system_operating_details.storage_stpt = 100

def controlFuncGenLimit():
    daq.system_operating_details.pv_stpt = daq.system_operating_details.ref

def controlFuncNone():
    daq.system_operating_details.storage_stpt = 0
    daq.system_operating_details.pv_stpt = 100

def time_of_use_func():
    with(open(COST_JSON_PATH))as cost_file:
        cost_cfg = json.load(cost_file)
        costs = cost_cfg["cost"]
    N = len(costs)
    avg_cost = sum(costs)/N
    abs_sum = sum([abs(x - avg_cost) for x in costs])
    print("battery_Storage : ",daq.system_operating_details.battery_storage_capacity)
    alpha = daq.system_operating_details.ref*daq.system_operating_details.battery_storage_capacity / abs_sum
    now_time = datetime.datetime.now()
    now_minutes = now_time.hour*60 + now_time.minute
    index = int(now_minutes / (24*60/N))
    print("power :",alpha * (costs[index] - avg_cost),"cost : ",costs[index],"avg : ",avg_cost,"alpha : ",alpha)
    daq.system_operating_details.storage_stpt= alpha * (costs[index] - avg_cost) *100/ daq.system_operating_details.battery_storage_capacity

def dr_based_batt_func():
    print(time.time())
    time_diff = daq.system_operating_details.event_end_time - daq.system_operating_details.event_start_time
    storage_capacity = daq.system_operating_details.battery_storage_capacity
    if(time.time() > daq.system_operating_details.event_start_time and time.time() < daq.system_operating_details.event_end_time):
        stpt = min(daq.system_operating_details.battery_storage_capacity  / time_diff,daq.system_operating_details.ref)
    else:
        stpt = -daq.system_operating_details.battery_storage_capacity  / (24 - time_diff/3600)
        pass
    daq.system_operating_details.storage_stpt = stpt

def daily_peak_th_base_func():
    daq.system_operating_details.storage_stpt = min(daq.system_operating_details.agg_batt_rated,abs(daq.system_operating_details.ref - daq.system_operating_details.aggGrid)) * (2*(daq.system_operating_details.aggGrid > daq.system_operating_details.ref) - 1)

def export_limit_func():
    grid_power = daq.system_operating_details.aggLoad - daq.system_operating_details.aggBatt - daq.system_operating_details.aggPV
    controlFuncConstPower()
    if(daq.system_operating_details.storage_stpt <= -100 and daq.system_operating_details.limit_export):
        daq.system_operating_details.pv_stpt = max(min(daq.system_operating_details.aggLoad - daq.system_operating_details.ref - daq.system_operating_details.storage_stpt*daq.system_operating_details.agg_batt_rated/100,daq.system_operating_details.agg_pv_rated),0)*100/daq.system_operating_details.agg_pv_rated
        daq.system_operating_details.pv_stpt = min(daq.system_operating_details.solar_max,daq.system_operating_details.pv_stpt)
    else:
        daq.system_operating_details.pv_stpt = 100    

def dg_pv_sync_func():
    tmp = daq.system_operating_details.aggLoad - daq.system_operating_details.dg_lim
    if(daq.system_operating_details.agg_batt_rated > 0):
        daq.system_operating_details.storage_stpt = max(min(tmp - daq.system_operating_details.aggPV,daq.system_operating_details.agg_batt_rated),-daq.system_operating_details.agg_batt_rated)*100/daq.system_operating_details.agg_batt_rated
        if(daq.system_operating_details.storage_stpt <= -100):
            print("tmp : ",tmp,daq.system_operating_details.aggLoad)
            daq.system_operating_details.pv_stpt = min(tmp - daq.system_operating_details.storage_stpt*daq.system_operating_details.agg_batt_rated/100,daq.system_operating_details.agg_pv_rated)*100/daq.system_operating_details.agg_pv_rated
    else:
        daq.system_operating_details.pv_stpt = max(min(daq.system_operating_details.agg_pv_rated,daq.system_operating_details.aggDG + daq.system_operating_details.aggPV - daq.system_operating_details.dg_lim),0)*100/daq.system_operating_details.agg_pv_rated 

def export_lim_export_priority_func():
    daq.system_operating_details.storage_stpt = min(0,daq.system_operating_details.aggBatt + min(0,daq.system_operating_details.ref+daq.system_operating_details.aggGrid))

def safety_control_func():
    if daq.system_operating_details.safety_control_mode == 0:
        pass
    elif daq.system_operating_details.safety_control_mode == 1:
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
    daq.system_operating_details.system_operating_mode = operatingMode_l2e[mode_str]
    if mode_str == "net_zero":
        daq.system_operating_details.ref = 0
        daq.system_operating_details.scs_min = 0
        daq.system_operating_details.scs_max = 1
        daq.system_operating_details.controlFunc = controlFuncConstPower
    if mode_str == "pv_charge_only":
        daq.system_operating_details.ref = 0
        daq.system_operating_details.scs_min = 0.5
        daq.system_operating_details.scs_max = 1
        daq.system_operating_details.controlFunc = controlPVChargeOnly
    if mode_str == "max_export":
        daq.system_operating_details.ref = 0
        daq.system_operating_details.scs_max = 0
        daq.system_operating_details.scs_min = 0
        daq.system_operating_details.controlFunc = controlFuncFullExport
    if mode_str == "power_backup":
        daq.system_operating_details.ref = 0
        daq.system_operating_details.scs_max = 1
        daq.system_operating_details.scs_min = 1
        daq.system_operating_details.controlFunc = controlFuncFullBackup
    if mode_str == "gen_limit":
        daq.system_operating_details.ref = ref
        daq.system_operating_details.scs_max = 1
        daq.system_operating_details.scs_min = 1
        daq.system_operating_details.controlFunc = controlFuncGenLimit
    if mode_str == "none":
        daq.system_operating_details.controlFunc = controlFuncNone
    if mode_str == "safety_control_mode":
        daq.system_operating_details.controlFunc = safety_control_func
    logging.warning(str(time.time()) + "change mode to: " + mode_str + " ref value : " + str(ref))

def SetController(Kp,Ki,Ts):
    daq.system_operating_details.Kp = Kp
    daq.system_operating_details.Ki = Ki
    daq.system_operating_details.Ts = Ts

def curtailStateToStpt():
    for device in daq.device_list:
        if device.stptCurve != None:
            device.control_data.setpt = device.stptCurve()
            data = {"AC Active_powerSetPct": device.control_data.setpt}
            device.writeDataToRegisters(data)

def processMQTTMessage(message : str):
    print("")
    print("==================")
    print("")
    with(open(CONTROL_JSON_PATH,'w') as control_file):
        control_file.write(message)
    print("====================")
    print("")

def startLiveData():
    daq.system_operating_details.live_data = True
    daq.system_operating_details.live_data_timer = 60
    print("live data started")

def stopLiveData():
    daq.system_operating_details.live_data = False
    daq.system_operating_details.live_data_timer = 0
    print("live data stopped")

def getActiveControlMode():
    if(daq.system_operating_details.aggDG > 0):
        daq.system_operating_details.system_operating_mode = systemOperatingModes.dg_pv_sync
        daq.system_operating_details.controlFunc = dg_pv_sync_func
        daq.system_operating_details.ref = daq.system_operating_details.dg_lim
    else:
        print("agg_dg somehow got : ",daq.system_operating_details.aggDG)
        try:
            with(open(CONTROL_JSON_PATH)) as control_file:
                control_json = json.load(control_file)
                if("mode" in control_json):
                    daq.system_operating_details.system_operating_mode = getattr(systemOperatingModes,control_json["mode"])
                    daq.system_operating_details.controlFunc = getattr(sys.modules[__name__],control_json["mode"] + "_func")
                    if "ref" in control_json["op_details"]:
                        daq.system_operating_details.ref = control_json["op_details"]["ref"]
                    else:
                        daq.system_operating_details.ref = 0  

                    if "Limit_export" in control_json["op_details"]:
                        daq.system_operating_details.limit_export = control_json["op_details"]["Limit_export"]
                    else:
                        daq.system_operating_details.limit_export = False
                    daq.system_operating_details.storage_min = -100
                    if "batt_to_load" in  control_json["op_details"]:
                        
                        daq.system_operating_details.storage_max = control_json["op_details"]["batt_to_load"] * 100

                    else:
                        daq.system_operating_details.storage_max = 0

                    if "storage_min" in control_json["op_details"]:
                        daq.system_operating_details.storage_min = control_json["op_details"]["storage_min"]
                    else:
                         daq.system_operating_details.storage_min = -100

                    if "storage_max" in control_json["op_details"]:
                        daq.system_operating_details.storage_max = control_json["op_details"]["storage_max"]
                    else:
                        daq.system_operating_details.storage_max = 100

                    if "solar_max" in control_json["op_details"]:
                        daq.system_operating_details.solar_max = control_json["op_details"]["solar_max"]
                    else:
                        daq.system_operating_details.solar_max  = 100  
                else:
                    daq.system_operating_details.controlFunc = None
                    print("mode from json : ",daq.system_operating_details.controlFunc)

        except (json.JSONDecodeError, FileNotFoundError):
            daq.system_operating_details.controlFunc = None
            print("Control JSON not found or invalid.")
    print("func is",daq.system_operating_details.controlFunc)

def runSysControlLoop():
    while True:
        daq.system_operating_details.aggPV,daq.system_operating_details.agg_pv_rated = daq.getAgg(deviceType.solar)
        daq.system_operating_details.aggBatt,daq.system_operating_details.agg_batt_rated, daq.system_operating_details.battery_storage_capacity = daq.getAgg(deviceType.battery)
        daq.system_operating_details.aggEV, aggEV = daq.getAgg(deviceType.EV)
        daq.getAggLoad()
        daq.getAggDGlim()
        daq.getAggDG()
        getActiveControlMode()
        if daq.system_operating_details.controlFunc != None:
            daq.system_operating_details.controlFunc()
            for device in daq.device_list:
                if device.device_type == deviceType.battery:
                    print("====battery device =====")
                    power = daq.system_operating_details.storage_stpt
                    data_msg = {"param" : "active_power","value":str(power)}
                    print(data_msg,daq.system_operating_details.storage_stpt)
                    device.encodeWrite(data_msg)
                if device.device_type == deviceType.solar:
                    device.control_data.power_pct_stpt.value = daq.system_operating_details.pv_stpt
                    data_msg = {"param" : "active_power","value":str(device.control_data.power_pct_stpt.value)}
                    print(data_msg)
                    device.encodeWrite(data_msg)
        time.sleep(1)