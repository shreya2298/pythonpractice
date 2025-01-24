from confluent_kafka import Consumer, KafkaException, KafkaError
import json
from data_processing import proc_data
from dbconn import conn




# Kafka Consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your broker(s)
    'group.id': 'my-consumer-group',        # Consumer group ID
    'auto.offset.reset': 'earliest',        # Start reading from the beginning if no offset is found
    'enable.auto.commit': True             # Automatically commit offsets
}

# Initialize Consumer
consumer = Consumer(consumer_config)

# Subscribe to topic
TOPIC_NAME = 'nifi-dummy'
consumer.subscribe([TOPIC_NAME])

print(f"Listening to topic '{TOPIC_NAME}'...")

try:
    while True:
        # Poll for a message
        msg = consumer.poll(timeout=1.0)  # Timeout in seconds
        if msg is None:
            continue  # No new message, continue polling
        else:
            # Successfully received a message
            message = json.loads(msg.value().decode('utf-8'))
            print(f"Received message: {proc_data(message)}")
            conn(proc_data(message))
except KeyboardInterrupt:
    print("\nStopped by user.")
finally:
    # Clean up and close the consumer
    consumer.close()

