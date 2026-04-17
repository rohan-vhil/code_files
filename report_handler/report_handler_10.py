import os
import json
import time
import requests
import logging
from datetime import datetime
import pytz
import enum
from threading import Lock
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

print("Report Handler Loaded")

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

            print("Ensuring TimescaleDB extension exists...")
            cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

            print("Ensuring table 'device_logs' exists...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS device_logs (
                    timestamp TIMESTAMPTZ NOT NULL,
                    device_id TEXT NOT NULL,
                    data JSONB NOT NULL
                );
            """)

            print("Configuring Hypertable...")
            cur.execute("""
                SELECT create_hypertable(
                    'device_logs',
                    'timestamp',
                    if_not_exists => TRUE,
                    migrate_data => TRUE,
                    chunk_time_interval => INTERVAL '1 day'
                );
            """)

            print("Setting Retention Policy...")
            cur.execute("""
                SELECT add_retention_policy(
                    'device_logs',
                    INTERVAL '90 days',
                    if_not_exists => TRUE
                );
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
        print("Initializing dataBank...")
        self.data_queue = []
        self.avg_data = {}
        self.report_url = ""
        self.report_period = 0
        self.report_type = reportType.average
        self.lock = Lock()

        self.db_params = {
            'dbname': 'solar_data',
            'user': 'user',
            'password': 'user',
            'host': '127.0.0.1',
            'port': '5432'
        }

        self.storage = LocalStorage(self.db_params)

        if not os.path.exists(unsent_json_path):
            print("Creating unsent_data.json file")
            with open(unsent_json_path, 'w') as f:
                json.dump([], f)

    def aggData(self, msg):
        self.data_queue.append(msg)

    def calculate_missing_power(self, data):
        if "voltage" in data and "current" in data and "total_power" not in data:
            data["total_power"] = round(data["voltage"] * data["current"], 2)
        return data

    def recursive_round(self, obj):
        if isinstance(obj, dict):
            return {k: self.recursive_round(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.recursive_round(v) for v in obj]
        elif isinstance(obj, (float, int)):
            return round(obj, 2)
        return obj

    def getAvg(self, device_id):
        print(f"Calculating average for device: {device_id}")
        self.avg_data[str(device_id)] = {"local_date": set_localdate()}
        device_data = [
            x.get(str(device_id))
            for x in self.data_queue
            if str(device_id) in x
        ]

        if len(device_data) > 0:
            for param in device_data[0]:
                if param != "type":
                    data = [x.get(param) for x in device_data]
                    if isinstance(data[0], (int, float)):
                        self.avg_data[str(device_id)][param] = round(sum(data) / len(data), 2)
                    elif isinstance(data[0], dict):
                        nested_avg = {}
                        for k in data[0].keys():
                            nested_values = [d.get(k, 0) for d in data if isinstance(d, dict)]
                            if all(isinstance(v, (int, float)) for v in nested_values):
                                nested_avg[k] = round(sum(nested_values) / len(nested_values), 2)
                            else:
                                nested_avg[k] = nested_values[-1]
                        self.avg_data[str(device_id)][param] = nested_avg
                    else:
                        self.avg_data[str(device_id)][param] = data[-1]
                else:
                    self.avg_data[str(device_id)]["type"] = device_data[0]["type"]

            self.avg_data[str(device_id)] = self.calculate_missing_power(
                self.avg_data[str(device_id)]
            )
            self.avg_data[str(device_id)] = self.recursive_round(self.avg_data[str(device_id)])

    def _load_unsent_data(self):
        with self.lock:
            try:
                with open(unsent_json_path, "r") as f:
                    return json.load(f)
            except:
                return []

    def _save_unsent_data(self, data):
        rounded_data = self.recursive_round(data)
        with self.lock:
            with open(unsent_json_path, "w") as f:
                json.dump(rounded_data, f, indent=2)

    def _append_to_unsent_data(self, new_data):
        print("Appending to unsent_data.json")
        unsent_data = self._load_unsent_data()
        unsent_data.append(new_data)
        self._save_unsent_data(unsent_data)

    def _clear_unsent_data(self):
        print("Clearing unsent_data.json")
        self._save_unsent_data([])

    def retry_unsent_data(self):
        unsent_list = self._load_unsent_data()
        if not unsent_list:
            return

        print(f"Attempting to resend {len(unsent_list)} unsent records...")
        still_unsent = []
        
        for record in unsent_list:
            try:
                response = requests.post(
                    self.report_url,
                    json=[record],
                    timeout=10,
                    verify=False
                )
                if response.status_code != 200:
                    still_unsent.append(record)
                else:
                    print("Successfully resent an old record.")
            except Exception:
                still_unsent.append(record)
        
        self._save_unsent_data(still_unsent)

    def runDataLoop(self):
        print("Starting runDataLoop...")
        while not os.path.exists(path_config.path_cfg.base_path + "devices.json"):
            print("Waiting for devices.json...")
            time.sleep(1)

        print("devices.json found. Entering main loop.")
        while True:
            try:
                with open(path_config.path_cfg.base_path + "reports_handling/report_cfg.json") as report_cfg:
                    report_config = json.load(report_cfg)

                self.report_url = report_config["report_url"]
                self.report_period = report_config["reporting_period"]
                self.avg_data = {}

                for device in ctrl.device_list:
                    self.getAvg(device.device_id)

                self.data_queue = []
                self.avg_data["timestamp"] = int(time.time())

                try:
                    response = requests.post(
                        self.report_url,
                        json=[self.avg_data],
                        timeout=10,
                        verify=False
                    )

                    if response.status_code == 200:
                        print("Data sent successfully:", self.avg_data)
                        for key, value in self.avg_data.items():
                            if key != "timestamp":
                                self.storage.save_device_data(key, value)
                        self.storage.flush()
                        self.retry_unsent_data()
                    else:
                        print(f"Server rejected data with status: {response.status_code}")
                        self._append_to_unsent_data(self.avg_data.copy())

                except requests.RequestException as e:
                    print("Network error:", e)
                    self._append_to_unsent_data(self.avg_data.copy())

                time.sleep(self.report_period)

            except Exception as e:
                print("Main Loop Error:", e)
                time.sleep(5)

data_handler: dataBank = None