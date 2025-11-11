from confluent_kafka import Producer
import time
import random

# Configuration for your Kafka broker
conf = {'bootstrap.servers': 'localhost:9092'}  # dictionary

# Create a producer instance
producer = Producer(conf)

topic_name = "my-topic"

print("Starting to send messages...")

try:
  for i in range(20):
    # Create a dynamic key to ensure message are distributed across partitions
    message_key = f'key-{random.randint(1, 4)}'
    message_value = f'Message number {i + 1} sent with key {message_key}'

    # Produce the message to the topic
    producer.produce(topic_name, key=message_key.encode(
        'utf-8'), value=message_value.encode('utf-8'))

    # Add a small delay to make the output more readable
    time.sleep(1)

  # Wait for all messages to be delivered to the broker
  producer.flush()

  print("Finished sending messages.")

except Exception as e:
  print(f'An error occurred: {e}')
