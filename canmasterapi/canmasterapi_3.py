'''Address Map Support (addr_map and ctrl_map)

Just like Modbus, define the CAN message IDs to poll and their data length/meaning.

Each addr_map["map"] will be a dictionary like:

addr_map = {
    "map": {
        "msg1": {"can_id": 0x100, "length": 8},
        "msg2": {"can_id": 0x101, "length": 4},
    }
}


getData() internal helper

Similar to Modbus, it will loop through addr_map['map'], wait for messages with matching IDs, and store their data.

Top-level getCANData() and writeCANData()

Will now work like Modbus version, returning {'read': [...], 'control': [...]}.
chatgpt'''


import sys
import enum
import time
import logging
from typing import Union

import can  # python-can library

sys.path.insert(0, "../")
sys.path.insert(0, "../control/")
from control import control_base as ctrl


# ------------------------- #
#    CAN DETAILS CLASSES    #
# ------------------------- #

class canDetails:
    channel = None
    bustype = None
    bitrate = 500000
    filters = []


# ------------------------- #
#         ENUMS             #
# ------------------------- #

class read_result(enum.IntEnum):
    success = 0
    fail_retry = 1
    fail_retry_later = 2
    fail_move = 3

    @classmethod
    def from_param(cls, obj):
        return int(obj)


class canErrorCodes(enum.IntEnum):
    no_error = 0
    connection_error = 1
    noresponse = 2
    invalid_frame = 3

    @classmethod
    def from_param(cls, obj):
        return int(obj)


# ------------------------- #
#      CAN DEVICE CLASS     #
# ------------------------- #

class canDevice(ctrl.systemDevice):
    can_comm_details: canDetails
    can_bus: can.Bus
    device_connected: bool
    addr_map: dict
    ctrl_map: dict
    rx_timeout: float

    def __init__(
        self,
        devicetype,
        commtype,
        channel,
        bustype="socketcan",
        bitrate=500000,
        address_map={},
        control_map={},
        filters=None,
        cfg={}
    ):
        super().__init__(devicetype, commtype, cfg)
        self.can_comm_details = canDetails()
        self.can_comm_details.channel = channel
        self.can_comm_details.bustype = bustype
        self.can_comm_details.bitrate = bitrate
        self.can_comm_details.filters = filters or []
        self.addr_map = address_map
        self.ctrl_map = control_map
        self.device_connected = False
        self.rx_timeout = 1.0
        self.can_bus = None

    def connect(self):
        try:
            logging.info(f"Connecting to CAN device on channel={self.can_comm_details.channel}")
            self.can_bus = can.Bus(
                channel=self.can_comm_details.channel,
                bustype=self.can_comm_details.bustype,
                bitrate=self.can_comm_details.bitrate
            )
            if self.can_comm_details.filters:
                self.can_bus.set_filters(self.can_comm_details.filters)
            self.device_connected = True
        except Exception as e:
            logging.error(f"CAN connect error: {e}")
            self.device_connected = False
        return self.device_connected

    def close_connection(self):
        try:
            if self.can_bus is not None:
                self.can_bus.shutdown()
            self.device_connected = False
            logging.info(f"Connection closed for CAN device {self.can_comm_details.channel}")
        except Exception as e:
            logging.warning(f"Error closing CAN connection: {e}")
        return False

    def readMessages(self, expected_ids, timeout=None):
        timeout = timeout or self.rx_timeout
        messages = []

        if not self.device_connected:
            logging.warning("CAN bus not connected")
            return messages

        try:
            start_time = time.time()
            while time.time() - start_time < timeout and len(messages) < len(expected_ids):
                msg = self.can_bus.recv(timeout=0.1)
                if msg is not None and msg.arbitration_id in expected_ids:
                    messages.append(msg)
        except Exception as e:
            logging.error(f"CAN read error: {e}")
        return messages

    def writeMessage(self, can_id, data_bytes, extended_id=False):
        if not self.device_connected:
            logging.warning("Cannot write: CAN bus not connected")
            return False
        try:
            msg = can.Message(arbitration_id=can_id, data=data_bytes, is_extended_id=extended_id)
            self.can_bus.send(msg)
            return True
        except Exception as e:
            logging.error(f"CAN write error: {e}")
            return False


# ------------------------- #
#     INTERNAL HELPERS      #
# ------------------------- #

def getData(addrmap: dict, device: canDevice):
    """
    Perform raw data reading from the given CAN device based on addrmap definition.
    addrmap should be in the format:
    {
      "map": {
         "msg1": {"can_id": 0x100, "length": 8},
         "msg2": {"can_id": 0x101, "length": 4}
      }
    }
    """
    data = []
    try:
        ids = [addrmap[m]["can_id"] for m in addrmap]
        messages = device.readMessages(expected_ids=ids)

        # Group messages by name
        for mname in addrmap:
            cid = addrmap[mname]["can_id"]
            msg = next((m for m in messages if m.arbitration_id == cid), None)
            if msg:
                data.append({
                    "name": mname,
                    "can_id": cid,
                    "data": list(msg.data)
                })
            else:
                data.append({
                    "name": mname,
                    "can_id": cid,
                    "data": None
                })

        device.read_error = False
    except Exception as e:
        device.read_error = True
        raise e

    return data


# ------------------------- #
#   PUBLIC HELPER FUNCTIONS #
# ------------------------- #

def getCANData(device: canDevice):
    candata = {'read': [], 'control': []}

    if not device.device_connected:
        if not device.connect():
            logging.warning(f"Failed to connect to CAN device {getattr(device, 'device_id', 'N/A')}. Skipping read cycle.")
            return candata

    try:
        if "map" in device.addr_map and device.addr_map["map"]:
            candata['read'] = getData(device.addr_map["map"], device)

        if "map" in device.ctrl_map and device.ctrl_map["map"]:
            candata['control'] = getData(device.ctrl_map["map"], device)

    except Exception as e:
        logging.error(f"CAN read cycle error for device {getattr(device, 'device_id', 'N/A')}: {e}")
        device.close_connection()
        candata['read'] = []
        candata['control'] = []

    return candata


def writeCANData(device: canDevice, can_id, data_bytes, extended_id=False):
    if not device.device_connected:
        if not device.connect():
            logging.warning(f"Failed to connect to CAN device {getattr(device, 'device_id', 'N/A')}. Skipping write.")
            return False

    success = device.writeMessage(can_id, data_bytes, extended_id)
    return success
