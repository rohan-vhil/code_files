'''Deployed on Asawa'''


import time
import requests
import logging
import control.control_base as ctrl

class DeviceStatusReporter:
    def __init__(self, poll_interval=60):
        self.last_known_statuses = {}
        self.poll_interval = poll_interval
        self.api_url = "https://app.enercog.com/ui/client/no-auth/device-status"
        self.session = requests.Session()
        self.logger = logging.getLogger("DeviceStatusReporter")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def check_and_report(self):
        try:
            live_power_data = ctrl.getLivePower()
        except Exception as e:
            self.logger.error(f"Could not get live power data: {e}")
            return
        
        if not live_power_data:
            self.logger.info("No live power data available to check.")
            return

        status_changes = {}
        for device_id, total_power in live_power_data.items():
            current_status = "online" if total_power else "offline"
            if self.last_known_statuses.get(device_id) != current_status:
                self.logger.info(f"STATUS CHANGE: Device '{device_id}' is now {current_status}.")
                status_changes[device_id] = current_status
        
        if not status_changes:
            return

        payload = [{device_id: status} for device_id, status in status_changes.items()]
        self.logger.info(f"Sending payload: {payload}")
        
        try:
            response = self.session.post(self.api_url, json=payload, timeout=10)
            response.raise_for_status()
            self.last_known_statuses.update(status_changes)
            self.logger.info(f"API update successful (Code: {response.status_code}).")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API update failed: {e}")

    def run(self):
        self.logger.info("Device Status Reporter thread started.")
        startup_delay = 20
        self.logger.info(f"Waiting for a {startup_delay}-second grace period before the first check.")
        time.sleep(startup_delay)

        while True:
            self.check_and_report()
            time.sleep(self.poll_interval)


'''
Asawa getAllData
 {"solar-inverter:M70A:RP703M260000": {"local_date": "2025-10-30", "type": "3ph_inverter", "L1_voltage": 231.42, "L2_voltage": 230.43, "L3_voltage": 230.03, "L1_current": 6.6, "L2_current": 6.23, "L3_current": 6.39, "L1_power": 1302.5, "L2_power": 1230.0, "L3_power": 1255.0, "total_power": 3787.5, "total_energy": 473733.0, "acfreq": 49.89, "input_power": 3982.5, "mppt": {"mppt1_voltage": 692.5, "mppt2_voltage": 721.4, "mppt3_voltage": 691.8, "mppt4_voltage": 699.8, "mppt5_voltage": 740.9, "mppt6_voltage": 756.8, "mppt1_current": 0.75, "mppt2_current": 0.81, "mppt3_current": 1.17, "mppt4_current": 1.24, "mppt5_current": 0.9, "mppt6_current": 0.68, "mppt1_power": 520, "mppt2_power": 590, "mppt3_power": 810, "mppt4_power": 870, "mppt5_power": 670, "mppt6_power": 520}, "string": {"string1_current": 0.38, "string2_current": 0.38, "string3_current": 0.0, "string4_current": 0.43, "string5_current": 0.4, "string6_current": 0.01, "string7_current": 0.5, "string8_current": 0.66, "string9_current": 0.0, "string10_current": 0.68, "string11_current": 0.54, "string12_current": 0.0, "string13_current": 0.47, "string14_current": 0.45, "string15_current": 0.02, "string16_current": 0.31, "string17_current": 0.38}}, "solar-inverter:MAC_100KTL:EGKME1Q00Y": {"local_date": "2025-10-30", "type": "3ph_inverter", "L1_voltage": 232.8, "L2_voltage": 233.23, "L3_voltage": 229.68, "L1_current": 6.58, "L2_current": 6.47, "L3_current": 6.3, "L1_power": 1530.58, "L2_power": 1510.0, "L3_power": 1446.88, "total_power": 2822.78, "total_energy": 10542.95, "acfreq": 49.88, "temperature": 49.0, "mppt": {"mppt1_voltage": 626.9, "mppt2_voltage": 648.1, "mppt3_voltage": 162.0, "mppt1_current": 0.5, "mppt2_current": 0.4, "mppt3_current": 0.0, "mppt1_power": 313.4, "mppt2_power": 259.2, "mppt3_power": 0.0}}, "load:MFM384:420217751": {"local_date": "2025-10-30", "type": "3ph_meter", "L1_voltage": 231.58, "L2_voltage": 231.18, "L3_voltage": 230.02, "L1_current": 314.2, "L2_current": 371.83, "L3_current": 345.86, "L1_power": 70380.1, "L2_power": 83274.85, "L3_power": 78521.38, "Pf": 0.98, "total_power": 232176.34, "total_energy": 4336781.75, "acfreq": 49.88, "apparent_power": 238270.61, "reactive_power": 52496.19}, "solar-inverter:M30A:RPI303FA0E1100": {"local_date": "2025-10-30", "type": "3ph_inverter", "L1_voltage": 231.53, "L2_voltage": 230.78, "L3_voltage": 230.12, "L1_current": 3.03, "L2_current": 3.06, "L3_current": 3.03, "L1_power": 507.0, "L2_power": 520.0, "L3_power": 514.25, "total_power": 1541.25, "total_energy": 190786.65, "acfreq": 49.89, "input_power": 17250.0, "mppt": {"mppt1_voltage": 599.7, "mppt2_voltage": 607.3, "mppt1_current": 1.59, "mppt2_current": 1.28, "mppt1_power": 954, "mppt2_power": 778}}, "solar-inverter:M70A:08X20B04827WM": {"local_date": "2025-10-30", "type": "3ph_inverter", "L1_voltage": 228.35, "L2_voltage": 227.7, "L3_voltage": 227.55, "L1_current": 2.36, "L2_current": 2.25, "L3_current": 2.2, "L1_power": 117.5, "L2_power": 95.0, "L3_power": 82.5, "total_power": 295.0, "total_energy": 1997.5, "acfreq": 49.89, "input_power": 360.0, "mppt": {"mppt1_voltage": 405.6, "mppt2_voltage": 199.9, "mppt3_voltage": 407.5, "mppt4_voltage": 387.4, "mppt5_voltage": 396.7, "mppt6_voltage": 200.0, "mppt1_current": 0.09, "mppt2_current": 0.0, "mppt3_current": 0.72, "mppt4_current": 0.1, "mppt5_current": 0.66, "mppt6_current": 0.0, "mppt1_power": 30, "mppt2_power": 0, "mppt3_power": 150, "mppt4_power": 30, "mppt5_power": 140, "mppt6_power": 0}, "string": {"string1_current": 0.03, "string2_current": 0.06, "string3_current": 0.0, "string4_current": 0.01, "string5_current": 0.0, "string6_current": 0.0, "string7_current": 0.03, "string8_current": 0.72, "string9_current": 0.01, "string10_current": 0.04, "string11_current": 0.06, "string12_current": 0.0, "string13_current": 0.01, "string14_current": 0.69, "string15_current": 0.0, "string16_current": 0.0, "string17_current": 0.0}}, "timestamp": 1761823483}
 
Device Status Json Format
[
    {
        "timestamp": 1759734074000,
        "meter_1": {
            "status": "online"
        },
        "inverter_1": {
            "status": "online",
            "mppt": {
                "mppt_1": "online",
                "mppt_2": "online"
            }
        },
        "inverter_2": {
            "status": "online",
            "mppt": {
                "mppt_1": "online",
                "mppt_2": "online",
                "mppt_3": "offline",
                "mppt_4": "online",
                "mppt_5": "online",
                "mppt_6": "online"
            },
            "string": {
                "string_1": "online",
                "string_2": "online",
                "string_3": "offline",
                "string_4": "offline",
                "string_5": "online",
                "string_6": "online",
                "string_7": "online",
                "string_8": "offline",
                "string_9": "online",
                "string_10": "online",
                "string_11": "online",
                "string_12": "offline",
                "string_13": "online",
                "string_14": "online",
                "string_15": "online",
                "string_16": "offline",
                "string_17": "online"
            }
        },
        "inverter_3": {
            "status": "online",
            "mppt": {
                "mppt_1": "online",
                "mppt_2": "online",
                "mppt_3": "online",
                "mppt_4": "online",
                "mppt_5": "online",
                "mppt_6": "online"
            },
            "string": {
                "string_1": "online",
                "string_2": "online",
                "string_3": "offline",
                "string_4": "online",
                "string_5": "online",
                "string_6": "online",
                "string_7": "online",
                "string_8": "online",
                "string_9": "offline",
                "string_10": "online",
                "string_11": "online",
                "string_12": "offline",
                "string_13": "online",
                "string_14": "online",
                "string_15": "online",
                "string_16": "online",
                "string_17": "online"
            }
        },
        "inverter_4": {
            "status": "online",
            "mppt": {
                "mppt_1": "online",
                "mppt_2": "online",
                "mppt_3": "offline"
            }
        }
    }
] 
 '''