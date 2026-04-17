'''
Update Python Script for JSON Validation

Here is the updated code with the JSON validation
logic implemented according to your schema and all comments removed'''


import pika
import time
from control import control_base as ctrl
import json

RABBITMQ_HOST = '3.110.77.154'
RABBITMQ_USER = 'enercog'
RABBITMQ_PASS = 'prod_enercog'
PUBLISH_INTERVAL_SEC = 5
PUBLISH_ROUTING_KEY = 'live_data.godrej_headspring'
LISTEN_QUEUE = 'command.godrej_headspring'
LISTEN_ROUTING_KEY = 'command.godrej_headspring'

EXCHANGE_NAME = 'rpi'

def isValidJson(json_string):
    try:
        data = json.loads(json_string)
        if not isinstance(data, dict):
            return False

        if "mode" not in data or "op_details" not in data:
            return False

        details = data["op_details"]
        if not isinstance(details, dict):
            return False

        required_int_keys = ["ref", "soc_max", "soc_min", "charge_rate_max", "discharge_rate_max"]
        for key in required_int_keys:
            if key not in details or not isinstance(details[key], int):
                return False

        required_bool_keys = ["allow_grid_charging", "allow_battery_to_grid"]
        for key in required_bool_keys:
            if key not in details or not isinstance(details[key], bool):
                return False

        if "rates" not in details or not isinstance(details["rates"], list) or len(details["rates"]) != 24:
            return False
        
        for rate in details["rates"]:
            if not isinstance(rate, (int, float)):
                return False

        if "schedule" not in details or not isinstance(details["schedule"], list):
            return False

        for item in details["schedule"]:
            if not isinstance(item, dict):
                return False
            if "start_time" not in item or "end_time" not in item or "setpoint" not in item:
                return False
            if not isinstance(item["start_time"], int) or not isinstance(item["end_time"], int):
                return False
            if not isinstance(item["setpoint"], (int, float)):
                return False

        return True
    except (json.JSONDecodeError, ValueError):
        return False
    return True

def start_subscriber():
    while True:
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
            channel = connection.channel()

            channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic', durable=True)
            result = channel.queue_declare(queue='command.godrej_headspring', durable=True, exclusive=False, arguments={"x-message-ttl": 60000})
            queue_name = result.method.queue

            channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=LISTEN_ROUTING_KEY)

            print(' [*] Waiting for messages. To exit press CTRL+C')

            def callback(ch, method, properties, body):
                try:
                    print(f" [x] Received {body.decode()}")
                    message = body.decode()
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                    if isValidJson(message):
                        print("json is valid")
                        ctrl.processMQTTMessage(body.decode())
                    else:
                        if(message == "LIVE_DATA_START"):
                            print("Starting Live Data")
                            ctrl.startLiveData()
                        elif(message == "LIVE_DATA_STOP"):
                            print("Stopping Live Data")
                            ctrl.stopLiveData()
                        
                        print("[!] Invalid JSON received")

                except Exception as e:
                    print(f"[!] Error in message callback: {e}")

            channel.basic_consume(queue=queue_name, on_message_callback=callback)
            channel.start_consuming()

        except Exception as e:
            print(f"[!] Subscriber error: {e}")
            time.sleep(5) 

if __name__ == '__main__':
    start_subscriber()