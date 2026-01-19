import json
import aio_pika
import datetime
import pika

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


def livdataHandler(data):
        print("livedata handler called with data : ",data)
        message = json.dumps(data)
        publish_message(message)
        return

def publish_message(message):
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
        channel = connection.channel()

        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic', durable=True)
                # Declare queues
        channel.queue_declare(queue=PUBLISH_ROUTING_KEY, durable=True, arguments={"x-message-ttl": 60000})
        channel.queue_declare(queue=LISTEN_QUEUE, durable=True, arguments={"x-message-ttl": 60000})

        # Bind queues to the topic exchange
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=PUBLISH_ROUTING_KEY, routing_key=PUBLISH_ROUTING_KEY)
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=LISTEN_QUEUE, routing_key=LISTEN_QUEUE)

        print(f"Connected. Publishing to '{PUBLISH_ROUTING_KEY}' via topic exchange '{EXCHANGE_NAME}'.")
        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=PUBLISH_ROUTING_KEY,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )

        print(f" [x] Sent '{message}' with topic '{PUBLISH_ROUTING_KEY}'")
        connection.close()

    except Exception as e:
        print(f"[!] Error publishing message: {e}")

if __name__ == '__main__':
    publish_message(MESSAGE)