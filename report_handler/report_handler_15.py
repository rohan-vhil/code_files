import os
import json
import time
import requests
import logging
from datetime import datetime
import pytz
import enum
from threading import Lock
import math
import psycopg2
from psycopg2.extras import execute_values

import path_config
import main_thread
from modbus_master import modbusmasterapi as mbus
from io_master import iomasterapi as io
from control import control_base as ctrl

config_path = "/home/edge_device/edge_device/installer_cfg/"
path = config_path + "installer_cfg.json"
unsent_json_path = "/home/edge_device/edge_device/edge_device/unsent_data.json"

logger = logging.getLogger('report_handler')
logger.setLevel(logging.ERROR)
file_handler = logging.FileHandler('report_handler_error.log')
logger.addHandler(file_handler)

class LocalStorage:
    def __init__(self, db_params):
        print("Initializing LocalStorage...")
        self.db_params = db_params
        self.buffer = []
        self.buffer_limit = 30
        self._init_db()

    def _get_connection(self):
        print(f"Connecting to PostgreSQL as user: {self.db_params.get('user')}...")
        return psycopg2.connect(**self.db_params)

    def _init_db(self):
        try:
            print("Initializing Database...")
            conn = self._get_connection()
            cur = conn.cursor()

            print("Ensuring table 'device_logs' exists...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS device_logs (
                    timestamp TIMESTAMPTZ NOT NULL,
                    device_id TEXT NOT NULL,
                    data JSONB NOT NULL
                );
            """)

            print("Ensuring indexes exist for fast querying...")
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_device_logs_timestamp 
                ON device_logs(timestamp DESC);
            """)

            conn.commit()
            cur.close()
            conn.close()
            print("Database initialized successfully")

        except Exception as e:
            print("PostgreSQL Init Error:", e)

    def save_device_data(self, device_id, data_dict):
        print(f"Buffering data for device: {device_id}")
        capture_time = datetime.now()
        clean_data = data_dict.copy()
        clean_data.pop('type', None)

        self.buffer.append(
            (capture_time, device_id, json.dumps(clean_data))
        )

        if len(self.buffer) >= self.buffer_limit:
            print(f"Buffer limit ({self.buffer_limit}) reached. Flushing to DB...")
            self.flush()

    def flush(self):
        if not self.buffer:
            print("Flush requested: Buffer is empty.")
            return

        try:
            print(f"Inserting {len(self.buffer)} records into device_logs...")
            conn = self._get_connection()
            cur = conn.cursor()

            execute_values(
                cur,
                "INSERT INTO device_logs (timestamp, device_id, data) VALUES %s",
                self.buffer
            )

            conn.commit()
            cur.close()
            conn.close()

            print("Batch insertion successful. Buffer cleared.")
            self.buffer = []

        except Exception as e:
            print("PostgreSQL Flush Error during insertion:", e)

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
        
        self.db_params = {
            'dbname': 'solar_data',
            'user': 'edge_device',
            'password': 'edge_device_jaibaba',
            'host': '127.0.0.1',
            'port': '5432'
        }
        self.storage = LocalStorage(self.db_params)

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
                    data = [x.get(param) for x in device_data if x.get(param) is not None]
                    if not data:
                        continue
                    
                    if isinstance(data[0], (int, float)):
                        avg_val = sum(data) / len(data)
                        if math.isnan(avg_val):
                            logger.error(f"NaN value detected for device {device_id} on parameter {param}")
                            self.avg_data[str(device_id)][param] = None
                        else:
                            self.avg_data[str(device_id)][param] = round(avg_val, 2)
                    else:
                        val = data[-1]
                        if isinstance(val, float) and math.isnan(val):
                            logger.error(f"NaN value detected for device {device_id} on parameter {param}")
                            self.avg_data[str(device_id)][param] = None
                        else:
                            self.avg_data[str(device_id)][param] = val
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
                    for key, value in self.avg_data.items():
                        if key != "timestamp":
                            self.storage.save_device_data(key, value)
                    self.storage.flush()
                else:
                    print(f"Failed to send current data (Status: {response.status_code}), saving for later.")
                    self._append_to_unsent_data(self.avg_data.copy())
            except requests.RequestException as e:
                print(f"Failed to send current data, saving for later: {e}")
                self._append_to_unsent_data(self.avg_data.copy())

            time.sleep(self.report_period)

data_handler: dataBank = None