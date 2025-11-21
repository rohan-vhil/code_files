import time
from pymodbus.client import ModbusSerialClient

# --- Configuration ---
# For Linux: '/dev/ttyUSB0', '/dev/ttyS0', etc.
# For Windows: 'COM3', 'COM4', etc.
SERIAL_PORT = '/dev/ttyUSB0'  # <-- CHANGE THIS to your serial port
BAUD_RATE = 9600             # <-- CHANGE THIS to match your device
SLAVE_ID = 1                 # From your example: 01
START_ADDRESS = 0            # From your example: 00 00
INPUT_COUNT = 8              # From your example: 00 08
POLL_INTERVAL = 1            # Time between reads, in seconds
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

try:
    if not client.connect():
        print(f"Error: Could not connect to serial port {SERIAL_PORT}")
        exit(1)  # Exit if connection fails

    print("Successfully connected. Starting continuous read loop...")
    print("Press Ctrl+C to stop.\n")

    while True:
        # This one line performs the "Send" and "Receive"
        # It corresponds to your "Send: 01 02 00 00 00 08 79 CC"
        response = client.read_discrete_inputs(
            address=START_ADDRESS,
            count=INPUT_COUNT,
            slave=SLAVE_ID
        )

        if response.isError():
            print(f"Modbus Error: {response}")
        else:
            # The 'bits' attribute holds the data.
            # It will be a list of 8 boolean values (True/False)
            
            print(f"--- Status Update @ {time.strftime('%Y-%m-%d %H:%M:%S')} ---")
            
            # Print the status clearly
            for i in range(INPUT_COUNT):
                channel_number = i + 1
                status = "TRIGGERED (ON)" if response.bits[i] else "Untriggered (OFF)"
                print(f"Channel {channel_number}: {status}")
            
            print(f"Raw bit list: {response.bits}\n")

        # Wait for the specified interval before polling again
        time.sleep(POLL_INTERVAL)

except KeyboardInterrupt:
    print("\nStopping continuous read...")

finally:
    client.close()
    print("Connection closed.")