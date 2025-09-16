'''You want a canmasterapi module with a similar structure and design pattern as your existing modbusmasterapi:

It should:

support CAN communication protocol

support read and (if device supports it) write operations

have separate CAN device detail classes

have a CAN device class inheriting from your ctrl.systemDevice base class

provide top-level helper functions like getCANData() and writeCANData()

It should follow the same structure, style, and naming pattern as your modbus code.'''


import sys
import enum
import time
import logging
from typing import Union

import can  # python-can library for CAN communication

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
    filters = []  # optional list of dictionaries [{'can_id':0x100, 'can_mask':0x7FF, 'extended':False}]


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
    rx_timeout: float

    def __init__(self, devicetype, commtype, channel, bustype="socketcan", bitrate=500000, filters=None, cfg={}):
        super().__init__(devicetype, commtype, cfg)
        self.can_comm_details = canDetails()
        self.can_comm_details.channel = channel
        self.can_comm_details.bustype = bustype
        self.can_comm_details.bitrate = bitrate
        self.can_comm_details.filters = filters or []
        self.device_connected = False
        self.rx_timeout = 1.0  # default 1 sec timeout
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

    def readMessages(self, expected_ids=None, timeout=None):
        """
        Read CAN messages. If expected_ids is provided, only return messages with those IDs.
        """
        timeout = timeout or self.rx_timeout
        messages = []

        if not self.device_connected:
            logging.warning("CAN bus not connected")
            return messages

        try:
            start_time = time.time()
            while time.time() - start_time < timeout:
                msg = self.can_bus.recv(timeout=0.1)
                if msg is not None:
                    if (expected_ids is None) or (msg.arbitration_id in expected_ids):
                        messages.append(msg)
        except Exception as e:
            logging.error(f"CAN read error: {e}")
        return messages

    def writeMessage(self, can_id, data_bytes, extended_id=False):
        """
        Write a CAN message frame.
        """
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
#     UTILITY FUNCTIONS     #
# ------------------------- #

def getCANData(device: canDevice, expected_ids=None):
    """
    This is the main CAN data acquisition function.
    It manages the connection lifecycle like getModbusData does.
    """
    candata = {'read': [], 'control': []}

    if not device.device_connected:
        if not device.connect():
            logging.warning(f"Failed to connect to CAN device {getattr(device, 'device_id', 'N/A')}. Skipping read cycle.")
            return candata

    try:
        # read all available messages
        msgs = device.readMessages(expected_ids)
        # you can split messages into 'read' and 'control' groups based on IDs if needed
        candata['read'] = msgs
        candata['control'] = []  # optional if you have specific control IDs
    except Exception as e:
        logging.error(f"CAN read cycle error for device {getattr(device, 'device_id', 'N/A')}: {e}")
        device.close_connection()
        candata['read'] = []
        candata['control'] = []

    # for CAN, you can keep the connection open like TCP, or close each cycle if needed
    return candata


def writeCANData(device: canDevice, can_id, data_bytes, extended_id=False):
    """
    Top-level helper to write data on CAN bus.
    """
    if not device.device_connected:
        if not device.connect():
            logging.warning(f"Failed to connect to CAN device {getattr(device, 'device_id', 'N/A')}. Skipping write.")
            return False

    success = device.writeMessage(can_id, data_bytes, extended_id)
    return success
