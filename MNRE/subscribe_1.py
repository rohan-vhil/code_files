'''MNRE Solar Data MQTT Connection
https://gemini.google.com/share/46969f017cee'''


import paho.mqtt.client as mqtt
import json
import ssl

CONFIG_FILE = 'config.json'

def load_config():
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

def update_config_key(key, value):
    config = load_config()
    config[key] = value
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=4)

def queue_history_request(date_val, index_val):
    """Adds a missing data request to the queue for the publisher to handle."""
    config = load_config()
    config['history_queue'].append({"DATE": date_val, "INDEX": index_val})
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=4)

def on_connect(client, userdata, flags, rc):
    config = userdata['config']
    if rc == 0:
        print("Subscriber connected successfully.")
        base_topic = f"{config['application']}/{config['solution']}/{config['imei']}"
        
        # Subscribe to required topics
        topics = [
            (f"{base_topic}/info", 1),
            (f"{base_topic}/OTP", 1),
            (f"{base_topic}/Config", 1),
            (f"{base_topic}/Ondemand", 1)
        ]
        client.subscribe(topics)
        print(f"Subscribed to: {[t[0] for t in topics]}")
    else:
        print(f"Connection failed: {rc}")

def on_message(client, userdata, msg):
    payload_str = msg.payload.decode('utf-8')
    topic = msg.topic
    print(f"\n[Command Received] Topic: {topic}")
    
    try:
        data = json.loads(payload_str)
        
        # 1. Handle OTP Topic
        if topic.endswith("/OTP"):
            if "COTP" in data:
                current_config = load_config()
                update_config_key('potp', current_config['cotp'])
                update_config_key('cotp', data['COTP'])
                print(f"OTP Rotated. New COTP: {data['COTP']}")

        # 2. Handle Config Topic (Configuration over the air)
        elif topic.endswith("/Config"):
            cmd_type = data.get("cmd")
            if cmd_type == "write":
                # Update local interval if server commands it
                if "UPDATEINTERVAL" in data:
                    update_config_key('stinterval', data['UPDATEINTERVAL'])
                    print(f"Updated STINTERVAL to {data['UPDATEINTERVAL']}")
                
                # Acknowledge execution by sending the exact JSON back (with msgid intact)
                client.publish(topic, json.dumps(data), qos=1)
                print(f"Acknowledged Config Write to server.")

        # 3. Handle Ondemand Topic
        elif topic.endswith("/Ondemand"):
            cmd_type = data.get("cmd")
            
            # Example: Server wants to read missing history data
            # Assuming the server sends a custom write command asking for history
            if cmd_type == "write" and "FETCH_HISTORY" in data:
                req_date = data.get("DATE")
                req_index = data.get("INDEX")
                if req_date and req_index:
                    print(f"Server requested missing data for DATE: {req_date}, INDEX: {req_index}")
                    queue_history_request(req_date, req_index)
            
            # Acknowledge Ondemand command back to server
            client.publish(topic, json.dumps(data), qos=1)

    except json.JSONDecodeError:
        print("Invalid JSON received.")

if __name__ == "__main__":
    config = load_config()
    client_id = f"d:{config['imei']}${config['solution']}$27"
    username = f"{config['imei']}${config['solution']}$27"
    
    client = mqtt.Client(client_id, userdata={'config': config})
    client.username_pw_set(username, password="your_password")
    client.tls_set(ca_certs=config['ca_cert'],
                   certfile=config['client_cert'],
                   keyfile=config['client_key'],
                   tls_version=ssl.PROTOCOL_TLSv1_2)

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(config['broker_url'], config['broker_port'], 60)
    client.loop_forever()