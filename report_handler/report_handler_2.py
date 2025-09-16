'''with these improvements:

Clean separation of payload building in build_report_payload()

Skips sending if no data is available (avoids sending empty payloads)

Keeps your unsent data retry logic the same'''



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
unsent_json_path = "/home/edge_device/edge_device/edge_device/unsent_reports.json"


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
        """Add new raw device data to the queue."""
        self.data_queue.append(msg)

    def _compute_device_avg(self, device_id):
        """Compute average of queued data for a single device."""
        device_data = [x.get(str(device_id)) for x in self.data_queue if str(device_id) in x]
        if not device_data:
            return None

        result = {"local_date": set_localdate()}
        for param in device_data[0]:
            if param == "type":
                result["type"] = device_data[0]["type"]
                continue

            data_values = [x.get(param) for x in device_data]
            if isinstance(data_values[0], (int, float)):
                result[param] = round(sum(data_values) / len(data_values), 2)
            else:
                result[param] = data_values

        return result

    def build_report_payload(self):
        """Build the full outgoing JSON payload.
        Returns: [payload_dict] or None if no data found.
        """
        payload = {"timestamp": int(time.time())}
        has_data = False

        for device in ctrl.device_list:
            avg_block = self._compute_device_avg(device.device_id)
            if avg_block:
                payload[str(device.device_id)] = avg_block
                has_data = True

        return [payload] if has_data else None

    # ---------------------------
    # Unsent data handling
    # ---------------------------
    def _load_unsent_data(self):
        with self.lock:
            if os.path.exists(unsent_json_path):
                with open(unsent_json_path, "r") as f:
                    try:
                        return json.load(f)
                    except json.JSONDecodeError:
                        return []
            return []

    def _save_unsent_data(self, data):
        with self.lock:
            with open(unsent_json_path, "w") as f:
                json.dump(data, f, indent=2)

    def _append_to_unsent_data(self, new_data):
        unsent_data = self._load_unsent_data()
        unsent_data.append(new_data)
        self._save_unsent_data(unsent_data)

    def _clear_unsent_data(self):
        self._save_unsent_data([])

    # ---------------------------
    # Main loop
    # ---------------------------
    def runDataLoop(self):
        # Wait until devices.json is present
        while not os.path.exists(path_config.path_cfg.base_path + "devices.json"):
            time.sleep(1)

        while os.path.exists(path_config.path_cfg.base_path + "devices.json"):
            with open(path_config.path_cfg.base_path + "reports_handling/report_cfg.json") as report_cfg:
                report_config = json.load(report_cfg)

            self.report_url = report_config["report_url"]
            self.report_period = report_config["reporting_period"]
            self.report_type = getattr(reportType, report_config["report_type"])

            # ⬇️ Use the new builder
            payload = self.build_report_payload()
            self.data_queue = []  # clear old data

            # Skip sending if no data
            if payload is None:
                print("No new data to send. Skipping this cycle.")
                time.sleep(self.report_period)
                continue

            # Try sending unsent first
            unsent_data = self._load_unsent_data()
            if unsent_data:
                try:
                    response = requests.post(self.report_url, json=unsent_data, verify=False)
                    if response.status_code == 200:
                        print(f"Successfully sent stored offline data: {json.dumps(unsent_data)}")
                        self._clear_unsent_data()
                except requests.RequestException as e:
                    print(f"Failed to send stored data: {e}")

            # Try sending current payload
            try:
                response = requests.post(self.report_url, json=payload, verify=False)
                if response.status_code == 200:
                    print(f"Successfully sent current data: {json.dumps(payload)}")
                else:
                    print(f"Failed to send current data (Status: {response.status_code}), saving for later.")
                    self._append_to_unsent_data(payload[0].copy())
            except requests.RequestException as e:
                print(f"Failed to send current data, saving for later: {e}")
                self._append_to_unsent_data(payload[0].copy())

            time.sleep(self.report_period)


# Global instance
data_handler: dataBank = None
