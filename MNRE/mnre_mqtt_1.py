import paho.mqtt.client as mqtt
import ssl
import time
from mnre_encoder import encode_dynamic_data, encode_daq_data

BROKER = "rms1.kusumiiot.co"
PORT = 8883
IMEI = "1234561234561234"
SOLUTION = "SolarMW"
CLIENT_ID = f"d:{IMEI}${SOLUTION}$27"
USERNAME = CLIENT_ID
PASSWORD = "YOUR_DEVICE_PASSWORD"

TOPIC_DATA = f"IIOT-1/{SOLUTION}/{IMEI}/Data"
TOPIC_ONDEMAND = f"IIOT-1/{SOLUTION}/{IMEI}/Ondemand"
TOPIC_CONFIG = f"IIOT-1/{SOLUTION}/{IMEI}/Config"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(TOPIC_ONDEMAND)
        client.subscribe(TOPIC_CONFIG)

def on_message(client, userdata, msg):
    pass

def publish_all_data():
    client = mqtt.Client(client_id=CLIENT_ID)
    client.username_pw_set(USERNAME, PASSWORD)
    
    client.tls_set(ca_certs="ca.crt", certfile="client.crt", keyfile="client.key", tls_version=ssl.PROTOCOL_TLSv1_2)
    
    client.on_connect = on_connect
    client.on_message = on_message
    
    client.connect(BROKER, PORT, 60)
    client.loop_start()
    
    inv_payload, meter_payload = encode_dynamic_data()
    
    client.publish(TOPIC_DATA, inv_payload, qos=1)
    client.publish(TOPIC_DATA, meter_payload, qos=1)
    
    daq_payload = encode_daq_data()
    client.publish(TOPIC_DATA, daq_payload, qos=1)
    
    time.sleep(2)
    client.loop_stop()
    client.disconnect()

if __name__ == "__main__":
    publish_all_data()