import time
from pymodbus.client import ModbusSerialClient

# --- Configuration ---
# For Linux: '/dev/ttyUSB0', '/dev/ttyS0', etc.
# For Windows: 'COM3', 'COM4', etc.
SERIAL_PORT = '/dev/ttyUSB0'  # <-- CHANGE THIS to your serial port
BAUD_RATE = 9600             # <-- CHANGE THIS to match your device
SLAVE_ID = 1                 # From your example: 01
RELAY_COUNT = 8
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

def turn_all_relays_off(client):
    """
    Safely turns all relays off using Function Code 15 (0x0F).
    This is the most reliable way to shut everything down.
    """
    print("\nAttempting to turn all 8 relays OFF...")
    try:
        # We use Function 15 (write_coils) here as it's the fastest
        # way to guarantee all relays are in a safe 'OFF' state.
        # This corresponds to "All relays off: 01 0F 00 00 00 08 01 00 FE 95"
        all_off_values = [False] * RELAY_COUNT
        response = client.write_coils(
            address=0,
            values=all_off_values,
            slave=SLAVE_ID
        )
        if response.isError():
            print(f"Modbus error trying to turn all relays off: {response}")
        else:
            print("All relays successfully turned OFF.")
    except Exception as e:
        print(f"Could not turn off relays (connection might be closed): {e}")


print(f"Connecting to Modbus RTU device on {SERIAL_PORT}...")

try:
    if not client.connect():
        print(f"Error: Could not connect to serial port {SERIAL_PORT}")
        exit(1)  # Exit if connection fails

    print("Successfully connected.")
    print("\n--- Relay Control Terminal ---")
    print("Commands:")
    print("  'on [number]'  -> (e.g., 'on 3')")
    print("  'off [number]' -> (e.g., 'off 5')")
    print("  'exit' or 'quit' to stop.")
    print("Press Ctrl+C at any time to stop and turn all relays off.")
    print("---------------------------------")

    while True:
        # 1. Get user input
        command_str = input("\nEnter command: ").strip().lower()

        # 2. Check for exit
        if command_str == "exit" or command_str == "quit":
            break

        # 3. Parse the command
        try:
            parts = command_str.split()
            if len(parts) != 2:
                print("Error: Invalid command. Please use format 'on 3' or 'off 5'.")
                continue

            action = parts[0]
            relay_num = int(parts[1])
            
            # 4. Validate input
            if action not in ['on', 'off']:
                print(f"Error: Unknown action '{action}'. Use 'on' or 'off'.")
                continue
                
            if not (1 <= relay_num <= RELAY_COUNT):
                print(f"Error: Relay number must be between 1 and {RELAY_COUNT}.")
                continue

            # 5. Execute the command
            # Convert 1-based number to 0-based address
            relay_address = relay_num - 1
            value_to_write = (action == 'on')  # True if 'on', False if 'off'

            print(f"Sending command: Relay {relay_num} (Address {relay_address}) -> {action.upper()}...")

            # This uses Function Code 05 (write_coil)
            response = client.write_coil(
                address=relay_address,
                value=value_to_write,
                slave=SLAVE_ID
            )

            if response.isError():
                print(f"Modbus Error: {response}")
            else:
                print(f"Successfully set Relay {relay_num} to {action.upper()}.")

        except ValueError:
            print("Error: Invalid relay number. Please enter a number (e.g., 'on 3').")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


except KeyboardInterrupt:
    print("\nCtrl+C detected! Stopping...")
    # The 'finally' block will handle turning all relays off.

finally:
    if client.is_socket_open():
        # This is the shutdown safety measure
        turn_all_relays_off(client)
        client.close()
        print("Connection closed.")
    else:
        print("Connection was already closed.")
