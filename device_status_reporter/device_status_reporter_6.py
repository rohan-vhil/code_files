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