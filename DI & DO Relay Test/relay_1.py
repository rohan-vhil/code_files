import time
from pymodbus.client import ModbusSerialClient

# --- Configuration ---
# For Linux: '/dev/ttyUSB0', '/dev/ttyS0', etc.
# For Windows: 'COM3', 'COM4', etc.
SERIAL_PORT = '/dev/ttyUSB0'  # <-- CHANGE THIS to your serial port
BAUD_RATE = 9600             # <-- CHANGE THIS to match your device
SLAVE_ID = 1                 # From your example: 01

# Write Relay Config (Function 0F)
WRITE_START_ADDRESS = 0      # From your example: 00 00
RELAY_COUNT = 8              # From your example: 00 08
DELAY_SECONDS = 960            # Wait time between ON and OFF
# ---------------------

# Initialize the Modbus RTU client
client = ModbusSerialClient(
    port=SERIAL_PORT,
    baudrate=BAUD_RATE,
    stopbits=1,
    bytesize=8,
    parity='N',
    timeout=1
)

print(f"Connecting to Modbus RTU device on {SERIAL_PORT}...")

# Define the two states we will write
# This matches "01 0F 00 00 00 08 01 FF BE D5" (All ON)
all_relays_on = [True] * RELAY_COUNT

# This matches "01 0F 00 00 00 08 01 00 FE 95" (All OFF)
all_relays_off = [False] * RELAY_COUNT

try:
    if not client.connect():
        print(f"Error: Could not connect to serial port {SERIAL_PORT}")
        exit(1)  # Exit if connection fails

    print("Successfully connected. Starting ON/OFF relay loop...")
    print("Press Ctrl+C to stop.\n")

    while True:
        # 1. Turn all 8 relays ON
        print(f"Turning all {RELAY_COUNT} relays ON...")
        on_response = client.write_coils(
            address=WRITE_START_ADDRESS,
            values=all_relays_on,
            slave=SLAVE_ID
        )
        if on_response.isError():
            print(f"Modbus Error (Write ON): {on_response}")
        
        # 2. Wait for 5 seconds
        print(f"Waiting {DELAY_SECONDS} seconds...")
        time.sleep(DELAY_SECONDS)

        # 3. Turn all 8 relays OFF
        print(f"Turning all {RELAY_COUNT} relays OFF...")
        off_response = client.write_coils(
            address=WRITE_START_ADDRESS,
            values=all_relays_off,
            slave=SLAVE_ID
        )
        if off_response.isError():
            print(f"Modbus Error (Write OFF): {off_response}")

        # 4. Wait for 5 seconds before repeating the loop
        print(f"Waiting {DELAY_SECONDS} seconds...")
        time.sleep(DELAY_SECONDS)


except KeyboardInterrupt:
    print("\nStopping loop...")
    print("Attempting to turn all relays OFF one last time.")
    # Good practice: try to turn relays off before exiting
    client.write_coils(WRITE_START_ADDRESS, all_relays_off, slave=SLAVE_ID)

finally:
    client.close()
    print("Connection closed.")
