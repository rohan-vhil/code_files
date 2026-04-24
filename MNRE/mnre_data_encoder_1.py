import control_base as ctrl
import json
from datetime import datetime

IMEI = "1234561234561234"
STINTERVAL = 15
POTP = "34123450"
COTP = "34123450"

with open('device_mapping.json', 'r') as f:
    DEVICE_MAP = json.load(f)

with open('key_mapping.json', 'r') as f:
    KEY_MAP = json.load(f)

def get_base_payload(vd_group):
    now = datetime.now()
    return {
        "VD": vd_group,
        "TIMESTAMP": now.strftime("%Y-%m-%d %H:%M:%S"),
        "MAXINDEX": 96,
        "INDEX": 1,
        "LOAD": 0,
        "STINTERVAL": STINTERVAL,
        "MSGID": "",
        "DATE": int(now.strftime("%y%m%d")),
        "IMEI": IMEI,
        "POTP": POTP,
        "COTP": COTP
    }

def encode_dynamic_data():
    raw_data = ctrl.getAllData()
    
    inv_payload = get_base_payload(5)
    meter_payload = get_base_payload(2)
    
    for device_id, device_data in raw_data.items():
        if device_id in DEVICE_MAP:
            dev_info = DEVICE_MAP[device_id]
            dev_type = dev_info["type"]
            dev_index = dev_info["index"]
            
            if dev_type == "inverter":
                target_payload = inv_payload
                mapping = KEY_MAP["inverter"]
                target_payload[f"ASN_3{dev_index}"] = device_id.split(":")[-1]
            elif dev_type == "meter":
                target_payload = meter_payload
                mapping = KEY_MAP["meter"]
                target_payload[f"ASN_2{dev_index}"] = device_id.split(":")[-1]
            else:
                continue
                
            for fw_key, fw_val in device_data.items():
                if fw_key in mapping:
                    mnre_key = f"{mapping[fw_key]}{dev_index}"
                    target_payload[mnre_key] = fw_val

    return json.dumps(inv_payload), json.dumps(meter_payload)

def encode_daq_data():
    dido_data = ctrl.getDIDOData()
    payload = get_base_payload(12)
    
    for key, val in dido_data.items():
        if key.startswith("di_"):
            idx = key.split("_")[1]
            payload[f"DI{idx}1"] = val
            
    return json.dumps(payload)