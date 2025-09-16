'''canmasterapi_1 Example'''

from canmasterapi import canDevice, getCANData, writeCANData
from control import control_base as ctrl

dev = canDevice(
    devicetype="sensor",
    commtype=ctrl.commType.can,
    channel="can0",
    bustype="socketcan",
    bitrate=500000
)

# Read messages
data = getCANData(dev, expected_ids=[0x100, 0x101])
for msg in data['read']:
    print(f"ID: {hex(msg.arbitration_id)}, Data: {msg.data.hex()}")

# Write message
writeCANData(dev, can_id=0x200, data_bytes=[0x01, 0x02, 0x03, 0x04])
