'''
----- Python Device Status Reporter - *updated* -----

Based on your new requirement, the device status should be determined by the success or failure of a Modbus read operation. If there's an error reading the device, it's considered "offline"; otherwise, it's "online".

Here is the updated device_status_reporter.py code. It maintains the original structure and naming while incorporating the new logic using your modbusmasterapi.py script.'''


import time
import requests
import logging
import control.control_base as ctrl
from modbus_master import modbusmasterapi

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
        status_changes = {}
        
        if hasattr(ctrl, 'device_list') and ctrl.device_list:
            for device in ctrl.device_list:
                device_id = device.device_id
                try:
                    modbusmasterapi.getModbusData(device)
                    current_status = "offline" if device.read_error else "online"
                except Exception as e:
                    self.logger.error(f"An unexpected error occurred while processing device {device_id}: {e}")
                    current_status = "offline"
                
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
2025-10-07 17:40:10,150 - DeviceStatusReporter - INFO - Device Status Reporter thread started.
2025-10-07 17:40:10,151 - DeviceStatusReporter - INFO - Waiting for a 20-second grace period before the first check.

# --- First Check (after 20s startup delay) ---
# Both devices are successfully read for the first time.
2025-10-07 17:40:30,225 - DeviceStatusReporter - INFO - STATUS CHANGE: Device 'inverter-01' is now online.
2025-10-07 17:40:30,310 - DeviceStatusReporter - INFO - STATUS CHANGE: Device 'meter-12' is now online.
2025-10-07 17:40:30,311 - DeviceStatusReporter - INFO - Sending payload: [{'inverter-01': 'online'}, {'meter-12': 'online'}]
2025-10-07 17:40:30,540 - DeviceStatusReporter - INFO - API update successful (Code: 200).

# --- Second Check (60 seconds later) ---
# Both devices are still online. Since there is no change, nothing is reported.
# (No output is printed in the console for this cycle)

# --- Third Check (60 seconds later) ---
# 'meter-12' fails to respond to the Modbus read command.
2025-10-07 17:42:30,415 - DeviceStatusReporter - INFO - STATUS CHANGE: Device 'meter-12' is now offline.
2025-10-07 17:42:30,416 - DeviceStatusReporter - INFO - Sending payload: [{'meter-12': 'offline'}]
2025-10-07 17:42:30,680 - DeviceStatusReporter - INFO - API update successful (Code: 200).

# --- Fourth Check (60 seconds later) ---
# 'meter-12' is reachable again.
2025-10-07 17:43:30,521 - DeviceStatusReporter - INFO - STATUS CHANGE: Device 'meter-12' is now online.
2025-10-07 17:43:30,522 - DeviceStatusReporter - INFO - Sending payload: [{'meter-12': 'online'}]
2025-10-07 17:43:30,755 - DeviceStatusReporter - INFO - API update successful (Code: 200).'''