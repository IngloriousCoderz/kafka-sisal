from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import time
import argparse


def run_historical_consumer(topic_name, bootstrap_servers):
  """
  A consumer that reads all available messages from the beginning of the topic, using a unique group ID.
  """
  # Configuration for your Kafka broker and consumer group
  conf = {
      "bootstrap.servers": bootstrap_servers,
      "group.id": f"historical-reader-{int(time.time())}",
      "auto.offset.reset": "earliest",  # Consume messages from the beginning
      "enable.auto.commit": False  # Disable automatic commit of offsets
  }

  # Create a consumer instance
  consumer = Consumer(conf)

  # Subscribe to the topic
  consumer.subscribe([topic_name])
  print("Starting to consume historical messages...")

  try:
    while True:
      # Poll for new messages every second
      msg = consumer.poll(1)

      if msg is None:
        continue

      if msg.error():
        # Handle potential errors
        if msg.error().code() == KafkaError._PARTITION_EOF:
          # End of partition, this is normal and not an error
          sys.stderr.write("%% %s [%d] reached the end of offset %s\n" % (
              msg.topic(), msg.partition(), msg.offset()))
          continue
        else:
          # Other errors
          raise KafkaException(msg.error())

      # Print the message's key and value
      print(
          f"Consumed historical message from partition {msg.partition()}: "
          f"key={msg.key().decode('utf-8')}, value={msg.value().decode('utf-8')}"
      )

      # Manually commit the offset of the message just processed.
      # This tells Kafka that we have succesfully processed messages up to this point.
      # Using synchronous commit is a safe choice, but can be slow.
      consumer.commit(asynchronous=False)
      print("Offsets manually committed.")

  except KeyboardInterrupt:
    # User interruption, graceful shutdown
    print("Stopping historical consumer...")

  finally:
    # Close the consumer when done. This also triggers a rebalance.
    consumer.close()


def run_realtime_consumer(topic_name, bootstrap_servers, consumer_group_id):
  """
  A consumer that reads new messages as they arrive, as part of the main consumer group.
  """
  # Configuration for your Kafka broker and consumer group
  conf = {
      "bootstrap.servers": bootstrap_servers,
      "group.id": consumer_group_id,
      "auto.offset.reset": "earliest",  # Consume messages from the beginning
      "enable.auto.commit": False  # Disable automatic commit of offsets
  }

  # Create a consumer instance
  consumer = Consumer(conf)

  # Subscribe to the topic
  consumer.subscribe([topic_name])
  print("Starting to consume messages...")

  try:
    while True:
      # Poll for new messages every second
      msg = consumer.poll(1)

      if msg is None:
        continue

      if msg.error():
        # Handle potential errors
        if msg.error().code() == KafkaError._PARTITION_EOF:
          # End of partition, this is normal and not an error
          sys.stderr.write("%% %s [%d] reached the end of offset %s\n" % (
              msg.topic(), msg.partition(), msg.offset()))
          continue
        else:
          # Other errors
          raise KafkaException(msg.error())

      # Print the message's key and value
      print(
          f"Consumed message from partition {msg.partition()}: "
          f"key={msg.key().decode('utf-8')}, value={msg.value().decode('utf-8')}"
      )

      # Manually commit the offset of the message just processed.
      # This tells Kafka that we have succesfully processed messages up to this point.
      # Using synchronous commit is a safe choice, but can be slow.
      consumer.commit(asynchronous=False)
      print("Offsets manually committed.")

  except KeyboardInterrupt:
    # User interruption, graceful shutdown
    print("Stopping consumer...")

  finally:
    # Close the consumer when done. This also triggers a rebalance.
    consumer.close()


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Kafka Consumer")
  parser.add_argument("--mode", type=str, default="realtime", choices=[
                      "realtime", "historical"], help="Choose consumer mode: historical (reads all messages) or realtime (joins the main group)")

  args = parser.parse_args()

  # Configuration for your Kafka broker
  topic_name = "python-topic"
  bootstrap_servers = "localhost:9092"
  consumer_group_id = "python_consumer_group"

  if args.mode == "historical":
    run_historical_consumer(topic_name, bootstrap_servers)
  else:
    run_realtime_consumer(topic_name, bootstrap_servers, consumer_group_id)
