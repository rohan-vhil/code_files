import time
from pymodbus.client import ModbusSerialClient

# --- Configuration ---
# For Linux: '/dev/ttyUSB0', '/dev/ttyS0', etc.
# For Windows: 'COM3', 'COM4', etc.
SERIAL_PORT = '/dev/ttyUSB0'  # <-- CHANGE THIS to your serial port
BAUD_RATE = 9600             # <-- CHANGE THIS to match your device
SLAVE_ID = 1                 # From your example: 01
POLL_INTERVAL = 1            # Time between reads, in seconds

# Read Input Config (Function 02)
READ_START_ADDRESS = 0
READ_INPUT_COUNT = 8

# Write Relay Config (Function 0F)
WRITE_START_ADDRESS = 0
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

    print("Successfully connected.")

    # --------------------------------------------------------------------
    # NEW PART: Write Relay Status (Function 0F)
    # --------------------------------------------------------------------
    print("\n--- Attempting to Write Relays (Function 0F) ---")

    # Define the 8 values (True=ON, False=OFF)
    # The list must have exactly 8 items for your device.

    # Example from your datasheet: "0-1 on; 2-7 off" (Byte 0x03)
    values_to_write = [
        True, True,         # 0 and 1 are ON
        False, False, False, # 2, 3, 4 are OFF
        False, False, False  # 5, 6, 7 are OFF
    ]
    
    # --- Uncomment one of the following to test ---

    # Example: "All relays on" (Byte 0xFF)
    # values_to_write = [True] * 8

    # Example: "All relays off" (Byte 0x00)
    # values_to_write = [False] * 8


    print(f"Writing values: {values_to_write}")
    
    # This corresponds to your "Send: 01 0F 00 00 00 08 01 03 BE 94"
    write_response = client.write_coils(
        address=WRITE_START_ADDRESS,
        values=values_to_write,
        slave=SLAVE_ID
    )

    if write_response.isError():
        print(f"Modbus Error on Write: {write_response}")
    else:
        print("Successfully wrote relays.")
        # The response '01 0F 00 00 00 01 94 0B' just confirms the write,
        # it doesn't return the data. Pymodbus handles this.
        print(f"Write response: {write_response}")

    # --------------------------------------------------------------------
    # EXISTING PART: Continuous Read Loop (Function 02)
    # --------------------------------------------------------------------
    print("\n--- Starting Continuous Read Loop (Function 02) ---")
    print("Press Ctrl+C to stop.\n")

    while True:
        # This one line performs the "Send" and "Receive"
        # It corresponds to your "Send: 01 02 00 00 00 08 79 CC"
        read_response = client.read_discrete_inputs(
            address=READ_START_ADDRESS,
            count=READ_INPUT_COUNT,
            slave=SLAVE_ID
        )

        if read_response.isError():
            print(f"Modbus Error on Read: {read_response}")
        else:
            # The 'bits' attribute holds the data.
            print(f"--- Status Update @ {time.strftime('%Y-%m-%d %H:%M:%S')} ---")
            
            # Print the status clearly
            for i in range(READ_INPUT_COUNT):
                channel_number = i + 1
                status = "TRIGGERED (ON)" if read_response.bits[i] else "Untriggered (OFF)"
                print(f"Channel {channel_number}: {status}")
            
            print(f"Raw bit list: {read_response.bits}\n")

        # Wait for the specified interval before polling again
        time.sleep(POLL_INTERVAL)

except KeyboardInterrupt:
    print("\nStopping continuous read...")

finally:
    client.close()
    print("Connection closed.")
