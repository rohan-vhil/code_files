from pymodbus.client import ModbusSerialClient
from pymodbus import framer
import time
import sys
import signal

# --- Default Configuration ---
DEFAULT_RTU_PORT = '/dev/ttyUSB0'
DEFAULT_BAUDRATE = 9600
DEFAULT_TIMEOUT = 3
DEFAULT_PARITY = 'N'

# Define the configuration for all 6 clients, now including 'register_type'
CLIENT_CONFIGS = [
    {'slave_id': 1, 'address': 3004, 'count': 20, 'name': 'solis_119', 'port': '/dev/ttyUSB0', 'baudrate': 9600, 'parity': 'N', 'register_type': 'input'},
    {'slave_id': 2, 'address': 3004, 'count': 20, 'name': 'solis_093', 'port': '/dev/ttyUSB0', 'baudrate': 9600, 'parity': 'N', 'register_type': 'input'},
    {'slave_id': 3, 'address': 3004, 'count': 20, 'name': 'solis_109', 'port': '/dev/ttyUSB0', 'baudrate': 9600, 'parity': 'N', 'register_type': 'input'},
    {'slave_id': 4, 'address': 3004, 'count': 20, 'name': 'solis_044', 'port': '/dev/ttyUSB0', 'baudrate': 9600, 'parity': 'N', 'register_type': 'input'},
    {'slave_id': 5, 'address': 147, 'count': 20, 'name': 'secure', 'port': '/dev/ttyUSB0', 'baudrate': 9600, 'parity': 'E', 'register_type': 'holding'},
    {'slave_id': 6, 'address': 100, 'count': 20, 'name': 'elemeasure', 'port': '/dev/ttyUSB0', 'baudrate': 9600, 'parity': 'E', 'register_type': 'holding'},
]

clients = []
for config in CLIENT_CONFIGS:
    client = ModbusSerialClient(
        port=config.get('port', DEFAULT_RTU_PORT),
        framer=framer.FramerType.RTU,
        baudrate=config.get('baudrate', DEFAULT_BAUDRATE),
        timeout=DEFAULT_TIMEOUT,
        parity=config.get('parity', DEFAULT_PARITY)
    )
    clients.append(client)

def cleanup_connections(signum=None, frame=None):
    print("\nKeyboard Interrupt detected. Closing all connections...")
    for i, client in enumerate(clients):
        if client.connected:
            client.close()
            print(f"Closed {CLIENT_CONFIGS[i]['name']} connection.")
    print("Exiting program.")
    sys.exit(0)

signal.signal(signal.SIGINT, cleanup_connections)

try:
    while True:
        for i, config in enumerate(CLIENT_CONFIGS):
            client = clients[i]
            client_name = config['name']
            slave_id = config['slave_id']
            address = config['address']
            count = config['count']
            port = config.get('port', DEFAULT_RTU_PORT)
            baudrate = config.get('baudrate', DEFAULT_BAUDRATE)
            parity = config.get('parity', DEFAULT_PARITY)
            register_type = config.get('register_type', 'input').lower()

            print(f"--- Starting {client_name} (Port: {port}, ID: {slave_id}, Type: {register_type.upper()}) ---")

            if client.connect():
                print(f"{client_name}: Connection Successful.")

                try:
                    data = None
                    if register_type == 'input':
                        # Function Code 4: Read Input Registers
                        data = client.read_input_registers(address=address, count=count, slave=slave_id)
                        
                    elif register_type == 'holding':
                        # Function Code 3: Read Holding Registers
                        data = client.read_holding_registers(address=address, count=count, slave=slave_id)
                        
                    else:
                        print(f"{client_name}: Invalid register_type '{register_type}'. Skipping read operation.")
                        
                    if data is not None:
                        if data.isError():
                            print(f"{client_name}: Reading Error: {data}")
                        else:
                            print(f"{client_name}: Data Read (Address {address}, Count {count}): {data.registers}")

                except Exception as e:
                    print(f"{client_name}: An unexpected reading exception occurred: {e}")
                
                client.close()
                print(f"{client_name}: Disconnected.")
                print("---------------------------------------")
                time.sleep(1)
            
            else:
                print(f"{client_name}: Connection Failed on port {port}. Check port access or device.")

except Exception as e:
    print(f"\nAn unhandled error occurred: {e}")
    cleanup_connections()