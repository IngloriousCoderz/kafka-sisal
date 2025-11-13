import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerConsumer {
  private final static String BOOTSTRAP_SERVERS = "localhost:9092";
  private final static String TOPIC = "java-topic";

  public static void main(String[] args) {
    // Run the producer in a separate thread
    Thread producerThread = new Thread(KafkaProducerConsumer::runProducer);
    producerThread.start();

    // Run the consumer in the main thread
    runConsumer();
  }

  private static void runProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      System.out.println("Starting producer...");

      for (int i = 0; i < 5; i++) {
        String key = (i % 2 == 0) ? "user_A" : "user_B"; // Use keys to demonstrate partitioning
        String value = "Message " + i + " from " + key;
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, key, value);

        producer.send(record, (metadata, exception) -> {
          if (exception == null) {
            System.out.printf("Produced message to topic '%s', partition %d, with key '%s' and offset %d%n",
                metadata.topic(), metadata.partition(), key, metadata.offset());
          } else {
            exception.printStackTrace();
          }
        }).get(); // Use .get() to make sure the message is sent before the loop continues
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }

    System.out.println("Producer finished.");
  }

  private static void runConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "java_consumer_group");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Start reading from the beginning of the topic

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      System.out.println("Starting consumer...");
      consumer.subscribe(Collections.singletonList(TOPIC));

      while (true) {
        // Poll for new records
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

        for (ConsumerRecord<String, String> record : records) {
          System.out.printf("Consumed message from topic '%s', partition %d, offset %d, key '%s', value '%s'%n",
              record.topic(), record.partition(), record.offset(), record.key(), record.value());
        }
      }
    }
  }
}
