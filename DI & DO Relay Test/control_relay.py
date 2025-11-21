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

# This list will hold the state of our 8 relays
# We start with all relays OFF
relay_states = [False] * RELAY_COUNT

try:
    if not client.connect():
        print(f"Error: Could not connect to serial port {SERIAL_PORT}")
        exit(1)  # Exit if connection fails

    print("Successfully connected. Starting chaser loop...")
    print("Press Ctrl+C to stop.\n")

    while True:
        # --- PART 1: Turn relays ON, one by one ---
        print("--- Sequence: Turning Relays ON ---")
        for i in range(RELAY_COUNT):
            relay_number = i + 1
            print(f"Turning Relay {relay_number} ON...")
            
            # Update the state list
            relay_states[i] = True
            
            # Send the *entire* list of 8 states to the device
            print(f"Sending states: {relay_states}")
            response = client.write_coils(
                address=WRITE_START_ADDRESS,
                values=relay_states,
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
            relay_number = i + 1
            print(f"Turning Relay {relay_number} OFF...")
            
            # Update the state list
            relay_states[i] = False
            
            # Send the *entire* list of 8 states to the device
            print(f"Sending states: {relay_states}")
            response = client.write_coils(
                address=WRITE_START_ADDRESS,
                values=relay_states,
                slave=SLAVE_ID
            )
            if response.isError():
                print(f"Modbus Error (Write OFF): {response}")

            # Wait before turning off the next one
            time.sleep(DELAY_SECONDS)
        
        # At this point, all relays are OFF
        print("All relays are now OFF. Restarting loop.\n")
        # The 'while True' loop will now repeat from the beginning


except KeyboardInterrupt:
    print("\nStopping loop...")
    print("Attempting to turn all relays OFF one last time.")
    try:
        # Good practice: try to turn all relays off before exiting
        all_off = [False] * RELAY_COUNT
        client.write_coils(WRITE_START_ADDRESS, all_off, slave=SLAVE_ID)
    except Exception as e:
        print(f"Could not turn off relays (connection might be closed): {e}")

finally:
    client.close()
    print("Connection closed.")
