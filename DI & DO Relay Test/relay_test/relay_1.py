import time
from pymodbus.client import ModbusSerialClient

# --- Configuration ---
# For Linux: '/dev/ttyUSB0', '/dev/ttyS0', etc.
# For Windows: 'COM3', 'COM4', etc.
SERIAL_PORT = '/dev/ttyUSB0'  # <-- CHANGE THIS to your serial port
BAUD_RATE = 9600             # <-- CHANGE THIS to match your device
SLAVE_ID = 1                 # From your example: 01

# Write Relay Config (Function 05)
RELAY_COUNT = 8              # We will loop from address 0 to 7
DELAY_SECONDS = 5            # Wait time between steps
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

    print("Successfully connected. Starting chaser loop (using Function 05)...")
    print("Press Ctrl+C to stop.\n")

    while True:
        # --- PART 1: Turn relays ON, one by one ---
        print("--- Sequence: Turning Relays ON ---")
        for i in range(RELAY_COUNT):
            # The relay address is 0, 1, 2... 7
            relay_address = i
            relay_number = i + 1
            
            print(f"Turning Relay {relay_number} (Address {relay_address}) ON...")

            # This single command uses Function Code 05.
            # Pymodbus handles converting 'True' to 0xFF00.
            # This corresponds to "Send: 01 05 00 00 FF 00 8C 3A" (for address 0)
            response = client.write_coil(
                address=relay_address,
                value=True,  # value=True means ON (0xFF00)
                slave=SLAVE_ID
            )
            
            if response.isError():
                print(f"Modbus Error (Write ON): {response}")

            # Wait before turning on the next one
            time.sleep(DELAY_SECONDS)

        # At this point, all relays are ON
        print("All relays are now ON.")
        
        # --- PART 2: Turn relays OFF, one by one ---
        print("\n--- Sequence: Turning Relays OFF ---")
        for i in range(RELAY_COUNT):
            relay_address = i
            relay_number = i + 1
            
            print(f"Turning Relay {relay_number} (Address {relay_address}) OFF...")
            
            # This single command uses Function Code 05.
            # Pymodbus handles converting 'False' to 0x0000.
            # This corresponds to "Send: 01 05 00 00 00 00 CD CA" (for address 0)
            response = client.write_coil(
                address=relay_address,
                value=False, # value=False means OFF (0x0000)
                slave=SLAVE_ID
            )
            
            if response.isError():
                print(f"Modbus Error (Write OFF): {response}")

            # Wait before turning off the next one
            time.sleep(DELAY_SECONDS)
        
        # At this point, all relays are OFF
        print("All relays are now OFF. Restarting loop.\n")
        
except KeyboardInterrupt:
    print("\nStopping loop...")
    print("Attempting to turn all 8 relays OFF one last time...")
    try:
        # Good practice: try to turn all relays off before exiting
        # We must send 8 separate "OFF" commands
        for i in range(RELAY_COUNT):
            client.write_coil(i, False, slave=SLAVE_ID)
            time.sleep(0.05) # Small delay between commands
    except Exception as e:
        print(f"Could not turn off relays (connection might be closed): {e}")

finally:
    client.close()
    print("Connection closed.")
