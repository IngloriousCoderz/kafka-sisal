from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient
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
  consumer.subscribe([topic_name])

  admin = AdminClient({'bootstrap.servers': bootstrap_servers})

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

    group_id = conf['group.id']
    admin.delete_consumer_groups([group_id])

# Function to read messages in real-time with the main group


def run_realtime_consumer(topic_name, bootstrap_servers, group_id):
  """
  A consumer that reads new messages as they arrive, as part of the main group.
  """
  conf = {
      'bootstrap.servers': bootstrap_servers,
      'group.id': group_id,
      'auto.offset.reset': 'earliest',  # Consume messages from the beginning
      'enable.auto.commit': False  # Disable automatic commit of offsets
  }

  consumer = Consumer(conf)
  consumer.subscribe([topic_name])

  print(
      f"Starting real-time consumer for group '{group_id}'. Waiting for new messages...")

  try:
    while True:
      msg = consumer.poll(1.0)

      if msg is None:
        continue

      if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
          sys.stderr.write('%% %s [%d] reached end of offset %s\n' % (
              msg.topic(), msg.partition(), msg.offset()))
          continue
        else:
          raise KafkaException(msg.error())

      print(
          f'Consumed real-time message from partition {msg.partition()}: '
          f"key={msg.key().decode('utf-8')}, value={msg.value().decode('utf-8')}"
      )

      consumer.commit(asynchronous=False)
      print("Offsets manually committed.")

  except KeyboardInterrupt:
    print("Stopping real-time consumer...")

  finally:
    consumer.close()


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="Kafka Consumer")
  parser.add_argument('--mode', type=str, default='realtime', choices=[
                      'realtime', 'historical'], help='Choose consumer mode: historical (reads all messages) or realtime (joins the main group)')

  args = parser.parse_args()

  # Configuration fo your Kafka broker
  topic_name = 'hello-world'
  bootstrap_servers = 'localhost:9092'
  group_id = 'my_first_consumer_group_python'

  if args.mode == 'historical':
    run_historical_consumer(topic_name, bootstrap_servers)
  else:
    run_realtime_consumer(topic_name, bootstrap_servers, group_id)
