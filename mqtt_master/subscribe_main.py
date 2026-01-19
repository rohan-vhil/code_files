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


# !!! IMPORTANT: This MUST match settings.RABBITMQ_EXCHANGE_NAME in your backend
# Find this value in your settings.py file.
EXCHANGE_NAME = 'rpi' # <-- IMPORTANT: CHANGE THIS


def isValidJson(json_string):
    # Implement your validation logic here
    try:
        json.loads(json_string)
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
                    #callFunc()
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