import time
from pymodbus.client import ModbusSerialClient

# --- Configuration ---
# For Linux: '/dev/ttyUSB0', '/dev/ttyS0', etc.
# For Windows: 'COM3', 'COM4', etc.
SERIAL_PORT = '/dev/ttyUSB1'  # <-- CHANGE THIS to your serial port
BAUD_RATE = 9600             # <-- CHANGE THIS to match your device
SLAVE_ID = 1                 # From your example: 01
RELAY_COUNT = 8
# ---------------------

def set_custom_shutdown_state(client):
    """
    Called on Ctrl+C: Sets Relays 1 & 2 ON, and 3-8 OFF.
    """
    print("\nExecuting custom shutdown (Relays 1 & 2 ON, 3-8 OFF)...")
    try:
        # State: [ON, ON, OFF, OFF, OFF, OFF, OFF, OFF]
        shutdown_values = [True, True, False, False, False, False, False, False]
        
        response = client.write_coils(
            address=0,
            values=shutdown_values,
            slave=SLAVE_ID
        )
        if response.isError():
            print(f"Modbus error trying to set shutdown state: {response}")
        else:
            print("Custom shutdown relay state set successfully.")
    except Exception as e:
        print(f"Could not set shutdown state (connection might be closed): {e}")

def turn_all_relays_off(client):
    """
    Called on normal 'exit': Safely turns all 8 relays OFF.
    """
    print("\nTurning all 8 relays OFF...")
    try:
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

# --- Main Program ---

# Initialize the Modbus RTU client
client = ModbusSerialClient(
    port=SERIAL_PORT,
    baudrate=BAUD_RATE,
    stopbits=1,
    bytesize=8,
    parity='N',
    timeout=1
)

# This flag tracks *how* we exit
# True = Ctrl+C (run custom sequence)
# False = 'exit' command (run all-off sequence)
run_custom_shutdown = True

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
    print("  'exit' or 'quit' to stop (turns all relays off).")
    print("Press Ctrl+C to stop (runs custom 1-2 ON sequence).")
    print("---------------------------------")

    while True:
        # 1. Get user input
        command_str = input("\nEnter command: ").strip().lower()

        # 2. Check for normal exit
        if command_str == "exit" or command_str == "quit":
            run_custom_shutdown = False  # Set flag for normal 'all-off'
            break # Exit the while loop

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

            # 5. Execute the command (Function 05)
            relay_address = relay_num - 1
            value_to_write = (action == 'on')

            # *** --- FIX IS HERE --- ***
            print(f"Sending command: Relay {relay_num} (Address {relay_address}) -> {action.upper()}...")
            
            response = client.write_coil(
                address=relay_address,
                value=value_to_write,
                slave=SLAVE_ID
            )

            if response.isError():
                print(f"Modbus Error: {response}")
            else:
                # *** --- AND FIX IS HERE --- ***
                print(f"Successfully set Relay {relay_num} to {action.upper()}.")

        except ValueError:
            print("Error: Invalid relay number. Please enter a number (e.g., 'on 3').")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

except KeyboardInterrupt:
    print("\nCtrl+C detected! Stopping...")
    # run_custom_shutdown remains True

finally:
    if client.is_socket_open():
        
        if run_custom_shutdown:
            # This is the Ctrl+C path
            set_custom_shutdown_state(client)
        else:
            # This is the 'exit' command path
            turn_all_relays_off(client)
            
        client.close()
        print("Connection closed.")
    else:
        print("Connection was already closed.")
