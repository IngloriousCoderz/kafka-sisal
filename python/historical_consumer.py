from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import argparse
import time

# Function to read all messages from the beginning of the topic


def run_historical_consumer(topic_name, boostrap_servers):
  """
  A consumer that reads all available messages from the beginning of the topic
  using a unique group ID.
  """

  # Use a unique group ID to ensure it's treated as a new consumer group
  # A new group will always start from 'earliest'
  conf = {
      'bootstrap.servers': boostrap_servers,
      'group.id': f'historical-reader-{int(time.time())}',
      'auto.offset.reset': 'earliest',  # Consume messages from the beginning
      'enable.auto.commit': False  # Disable automatic commit of offsets
  }

  consumer = Consumer(conf)
  consumer.subscribe(topic_name)

  print("Starting historical consumer. Reading all previous messages...")

  try:
    while True:
      # Poll for new messages every second
      msg = consumer.poll(1.0)

      if msg is None:
        continue

      if msg.error():
        # Handle any potential errors
        if msg.error().code() == KafkaError._PARTITION_EOF:
          # End of partition, this is normal and not an error
          sys.stderr.write('%% %s [%d] reached end of offset %s\n' % (
              msg.topic(), msg.partition(), msg.offset()))
          continue
        else:
          # Other errors
          raise KafkaException(msg.error())

      # Print the message's key and value
      print(
          f'Consumed historical message from partition {msg.partition()}: '
          f"key={msg.key().decode('utf-8')}, value={msg.value().decode('utf-8')}"
      )

      # Manually commit the offset of the message just processed.
      # This tells Kafka that we have successfully processed messages up to this point.
      # Using synchronous commit is a safe choice, but can be slow.
      consumer.commit(asynchronous=False)
      print("Offsets manually committed.")

  except KeyboardInterrupt:
    # User interruption, graceful shutdown
    print("Stopping historical consumer...")

  finally:
    # CLose the consumer when done. This also triggers a rebalance.
    consumer.close()
