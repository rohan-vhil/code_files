import time
import requests
import logging
import control.control_base as ctrl

class DeviceStatusReporter:
    """
    A class to monitor device online/offline status and report changes to an API.
    Designed to be run in a separate thread.
    """
    def __init__(self, poll_interval=60):
        self.last_known_statuses = {}
        self.poll_interval = poll_interval
        self.api_url = "https://example.com/getDeviceStatus"
        # Use a dedicated logger to avoid conflicts
        self.logger = logging.getLogger("DeviceStatusReporter")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        if not self.logger.handlers:
            self.logger.addHandler(handler)

    def check_and_report(self):
        """
        Fetches live power, compares to last known statuses, and sends updates
        to an API if any status has changed.
        """
        try:
            live_power_data = ctrl.getLivePower()
        except Exception as e:
            self.logger.error(f"Could not get live power data from control_base: {e}")
            return
        
        if not live_power_data:
            self.logger.info("No live power data available to check.")
            return

        status_updates_to_send = []

        for device_id, total_power in live_power_data.items():
            current_status = "online" if total_power != 0 else "offline"
            previous_status = self.last_known_statuses.get(device_id)

            if previous_status != current_status:
                self.logger.info(f"STATUS CHANGE: Device '{device_id}' is now {current_status}.")
                status_updates_to_send.append({device_id: current_status})
                self.last_known_statuses[device_id] = current_status

        if status_updates_to_send:
            # THIS IS THE NEW LINE THAT PRINTS THE PAYLOAD
            self.logger.info(f"**Payload to send:** {status_updates_to_send}")
            
            self.logger.info(f"Sending {len(status_updates_to_send)} status update(s) to API...")
            try:
                response = requests.post(self.api_url, json=status_updates_to_send, timeout=10)
                response.raise_for_status()
                self.logger.info(f"API update successful (Code: {response.status_code}).")
            except requests.exceptions.RequestException as e:
                self.logger.error(f"API update failed: {e}")

    def run(self):
        """The main entry point for the thread, runs an infinite loop."""
        self.logger.info("Device Status Reporter thread started.")
        while True:
            self.check_and_report()
            time.sleep(self.poll_interval)