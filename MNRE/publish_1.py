'''MNRE Solar Data MQTT Connection
https://gemini.google.com/share/46969f017cee'''


import paho.mqtt.client as mqtt
import json
import ssl
import time
import sqlite3
from datetime import datetime

CONFIG_FILE = 'config.json'
DB_FILE = 'local_storage.db'

def setup_database():
    """Initializes local SQLite database for History Data Push Mode"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS local_data 
                 (date_val INT, index_val INT, vd_group INT, payload TEXT, 
                  PRIMARY KEY(date_val, index_val, vd_group))''')
    conn.commit()
    conn.close()

def save_to_db(date_val, index_val, vd_group, payload_dict):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    payload_str = json.dumps(payload_dict)
    c.execute('''INSERT OR REPLACE INTO local_data (date_val, index_val, vd_group, payload) 
                 VALUES (?, ?, ?, ?)''', (date_val, index_val, vd_group, payload_str))
    conn.commit()
    conn.close()

def load_config():
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

def clear_history_queue():
    config = load_config()
    config['history_queue'] = []
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=4)

def process_history_queue(client, publish_topic):
    """Checks config for missing data requests and publishes them from SQLite"""
    config = load_config()
    queue = config.get('history_queue', [])
    
    if not queue:
        return

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    for req in queue:
        req_date = req['DATE']
        req_index = req['INDEX']
        
        print(f"Processing missing data request for DATE: {req_date}, INDEX: {req_index}")
        
        c.execute("SELECT payload FROM local_data WHERE date_val=? AND index_val=?", (req_date, req_index))
        rows = c.fetchall()
        
        for row in rows:
            history_payload = json.loads(row[0])
            # Set LOAD to 1 or whatever value the server requires to identify historical data
            history_payload["LOAD"] = 1 
            client.publish(publish_topic, json.dumps(history_payload), qos=1)
            print(f"Published historical data for VD: {history_payload['VD']}")
            time.sleep(0.5)

    conn.close()
    clear_history_queue() # Clear queue after processing

# ... [Keep the get_meter_payload, get_inverter_payload functions from the previous response] ...
def get_base_payload(config, vd_group, index_val, date_val, now_str):
    return {
        "VD": vd_group,
        "TIMESTAMP": now_str,
        "MAXINDEX": int(1440 / config['stinterval']), # Dynamically calculate MAXINDEX
        "INDEX": index_val,
        "LOAD": 0,
        "STINTERVAL": config['stinterval'],
        "MSGID": "",
        "DATE": date_val,
        "IMEI": config['imei'],
        "POTP": config['potp'],
        "COTP": config['cotp']
    }

if __name__ == "__main__":
    setup_database()
    config = load_config()
    
    client_id = f"d:{config['imei']}${config['solution']}$27"
    username = f"{config['imei']}${config['solution']}$27"
    
    client = mqtt.Client(client_id)
    client.username_pw_set(username, password="your_password")
    client.tls_set(ca_certs=config['ca_cert'], certfile=config['client_cert'],
                   keyfile=config['client_key'], tls_version=ssl.PROTOCOL_TLSv1_2)

    client.connect(config['broker_url'], config['broker_port'], 60)
    client.loop_start()

    publish_topic = f"{config['application']}/{config['solution']}/{config['imei']}/Data"
    
    current_index = 1 # In a real scenario, calculate this based on the time of day

    try:
        while True:
            current_config = load_config()
            now = datetime.now()
            now_str = now.strftime("%Y-%m-%d %H:%M:%S")
            date_val = int(now.strftime("%y%m%d"))
            
            # Generate Payloads
            meter_data = get_base_payload(current_config, 2, current_index, date_val, now_str)
            # ... Add meter specific data here
            
            inverter_data = get_base_payload(current_config, 5, current_index, date_val, now_str)
            # ... Add inverter specific data here

            # Save to Local SQLite Database (History Mode Provision)
            save_to_db(date_val, current_index, 2, meter_data)
            save_to_db(date_val, current_index, 5, inverter_data)
            
            # Publish Real-time Data
            client.publish(publish_topic, json.dumps(meter_data), qos=1)
            client.publish(publish_topic, json.dumps(inverter_data), qos=1)
            print(f"Published real-time data for INDEX: {current_index}")
            
            # Check and process any requested missing history data
            process_history_queue(client, publish_topic)
            
            current_index += 1
            if current_index > (1440 / current_config['stinterval']):
                current_index = 1 # Reset index at midnight
            
            print(f"Sleeping for {current_config['stinterval']} minutes...")
            time.sleep(current_config['stinterval'] * 60)
            
    except KeyboardInterrupt:
        print("Stopping publisher...")
    finally:
        client.loop_stop()
        client.disconnect()