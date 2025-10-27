import os
import json
import time
import requests
import logging
from datetime import datetime
import pytz
import enum
from threading import Lock

import path_config
import main_thread
from modbus_master import modbusmasterapi as mbus
from io_master import iomasterapi as io
from control import control_base as ctrl

config_path = "/home/edge_device/edge_device/installer_cfg/"
path = config_path + "installer_cfg.json"
unsent_json_path = "/home/edge_device/edge_device/unsent_data.json"


class reportType(enum.IntEnum):
    average = 0
    all = 1

    @classmethod
    def from_param(cls, obj):
        return int(obj)


def set_localdate():
    epoch_timestamp = int(time.time())
    timezone_str = 'Asia/Kolkata'
    utc_date = datetime.fromtimestamp(epoch_timestamp, tz=pytz.utc)
    local_timezone = pytz.timezone(timezone_str)
    local_date = utc_date.astimezone(local_timezone)
    return local_date.strftime('%Y-%m-%d')


class dataBank:
    def __init__(self):
        self.data_queue = []
        self.avg_data = {}
        self.report_url = ""
        self.report_period = 0
        self.report_type = reportType.average
        self.lock = Lock()
        if not os.path.exists(unsent_json_path):
            with open(unsent_json_path, 'w') as f:
                json.dump([], f)

    def aggData(self, msg):
        self.data_queue.append(msg)

    def getAvg(self, device_id):
        self.avg_data[str(device_id)] = {"local_date": set_localdate()}
        device_data = [x.get(str(device_id)) for x in self.data_queue if str(device_id) in x]

        if len(device_data) > 0:
            for param in device_data[0]:
                if param != "type":
                    data = [x.get(param) for x in device_data]
                    if isinstance(data[0], (int, float)):
                        self.avg_data[str(device_id)][param] = round(sum(data) / len(data), 2)
                    else:
                        self.avg_data[str(device_id)][param] = data[-1]
                else:
                    self.avg_data[str(device_id)]["type"] = device_data[0]["type"]

    def _load_unsent_data(self):
        with self.lock:
            if os.path.exists(unsent_json_path):
                with open(unsent_json_path, "r") as f:
                    try:
                        loaded_data = json.load(f)
                        print(f"Loaded unsent data from file: {json.dumps(loaded_data)}")
                        return loaded_data
                    except json.JSONDecodeError:
                        print("Error decoding unsent_data.json, returning empty list.")
                        return []
            return []

    def _save_unsent_data(self, data):
        with self.lock:
            with open(unsent_json_path, "w") as f:
                json.dump(data, f, indent=2)

    def _append_to_unsent_data(self, new_data):
        unsent_data = self._load_unsent_data()
        unsent_data.append(new_data)
        print(f"Appending new data to unsent list: {json.dumps(new_data)}")
        self._save_unsent_data(unsent_data)

    def _clear_unsent_data(self):
        self._save_unsent_data([])

    def runDataLoop(self):
        print("-------into runDataLoop-------")
        while not os.path.exists(path_config.path_cfg.base_path + "devices.json"):
            time.sleep(1)

        while os.path.exists(path_config.path_cfg.base_path + "devices.json"):
            with open(path_config.path_cfg.base_path + "reports_handling/report_cfg.json") as report_cfg:
                report_config = json.load(report_cfg)

            self.report_url = report_config["report_url"]
            self.report_period = report_config["reporting_period"]
            self.report_type = getattr(reportType, report_config["report_type"])

            self.avg_data = {}
            for device in ctrl.device_list:
                self.getAvg(device.device_id)

            self.data_queue = []
            self.avg_data["timestamp"] = int(time.time())

            unsent_data = self._load_unsent_data()
            print("-------json loaded-------")

            if unsent_data:
                try:
                    response = requests.post(self.report_url, json=unsent_data, verify=False)
                    if response.status_code == 200:
                        print(f"Successfully sent stored offline data: {json.dumps(unsent_data)}")
                        self._clear_unsent_data()
                    else:
                        print(f"Failed to send stored offline data (Status: {response.status_code})")
                except requests.RequestException as e:
                    print(f"Failed to send stored data: {e}")

            try:
                response = requests.post(self.report_url, json=[self.avg_data], verify=False)
                if response.status_code == 200:
                    print(f"Successfully sent current data: {json.dumps(self.avg_data)}")
                else:
                    print(f"Failed to send current data (Status: {response.status_code}), saving for later.")
                    self._append_to_unsent_data(self.avg_data.copy())
            except requests.RequestException as e:
                print(f"Failed to send current data, saving for later: {e}")
                self._append_to_unsent_data(self.avg_data.copy())

            time.sleep(self.report_period)


data_handler: dataBank = None